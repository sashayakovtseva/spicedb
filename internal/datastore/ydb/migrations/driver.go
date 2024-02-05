package migrations

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydbOtel "github.com/ydb-platform/ydb-go-sdk-otel"
	ydbZerolog "github.com/ydb-platform/ydb-go-sdk-zerolog"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/authzed/spicedb/internal/datastore/ydb/common"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/truetime"
)

var _ migrate.Driver[TableClientWithOptions, TxActorWithOptions] = (*YDBDriver)(nil)

const (
	queryLoadVersion = `
SELECT 
    version_num,
    created_at_unix_nano
FROM 
    schema_version 
ORDER BY 
    created_at_unix_nano DESC 
LIMIT 1
`

	queryWriteVersion = `
DECLARE $newVersion as Utf8;
DECLARE $createAtUnixNano as Int64;
INSERT INTO schema_version(version_num, created_at_unix_nano) VALUES ($newVersion, $createAtUnixNano);
`
)

type options struct {
	tablePathPrefix string
}

// YDBDriver implements a schema migration facility for use in SpiceDB's YDB datastore.
type YDBDriver struct {
	db *ydb.Driver
	options
}

// NewYDBDriver creates a new driver with active connections to the database specified.
func NewYDBDriver(ctx context.Context, dsn string) (*YDBDriver, error) {
	parsedDSN := common.ParseDSN(dsn)

	db, err := ydb.Open(ctx, dsn,
		ydbZerolog.WithTraces(&log.Logger, trace.DatabaseSQLEvents),
		ydbOtel.WithTraces(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate YDBDriver: %w", err)
	}

	return &YDBDriver{
		db: db,
		options: options{
			tablePathPrefix: parsedDSN.TablePathPrefix,
		},
	}, nil
}

// Version returns the current version of the schema in the backing datastore.
// If the datastore is brand new, version returns the empty string without an error.
func (d *YDBDriver) Version(ctx context.Context) (string, error) {
	var (
		loaded            string
		createdAtUnixNano int64 // don't really need this variable
	)

	err := d.db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, res, err := s.Execute(
			ctx,
			table.DefaultTxControl(),
			common.AddTablePrefix(queryLoadVersion, d.tablePathPrefix),
			nil,
		)
		if err != nil {
			return err
		}
		defer res.Close()

		if err := res.NextResultSetErr(ctx); err != nil {
			return err
		}
		for res.NextRow() {
			if err := res.Scan(&loaded, &createdAtUnixNano); err != nil {
				return err
			}
		}
		return res.Err()
	})
	if err != nil {
		if isMissingTableError(err) || errors.Is(err, io.EOF) {
			return "", nil
		}
		return "", fmt.Errorf("failed to load alembic revision: %w", err)
	}

	return loaded, nil
}

func (d *YDBDriver) WriteVersion(ctx context.Context, tx TxActorWithOptions, version, _ string) error {
	res, err := tx.tx.Execute(
		ctx,
		common.AddTablePrefix(queryWriteVersion, tx.opts.tablePathPrefix),
		table.NewQueryParameters(
			table.ValueParam("$newVersion", types.TextValue(version)),
			table.ValueParam("$createAtUnixNano", types.Int64Value(truetime.UnixNano())),
		),
	)
	if err != nil {
		return fmt.Errorf("failed to insert new version row: %w", err)
	}
	defer res.Close()

	return res.Err()
}

// Conn returns the underlying table client instance for this driver.
func (d *YDBDriver) Conn() TableClientWithOptions {
	return TableClientWithOptions{
		client: d.db.Table(),
		opts:   d.options,
	}
}

func (d *YDBDriver) RunTx(ctx context.Context, f migrate.TxMigrationFunc[TxActorWithOptions]) error {
	return d.db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		return f(ctx, TxActorWithOptions{
			tx:   tx,
			opts: d.options,
		})
	})
}

// Close disposes the driver.
func (d *YDBDriver) Close(ctx context.Context) error {
	return d.db.Close(ctx)
}

func isMissingTableError(err error) bool {
	const ydbMissingTableIssueCode = 2003

	var b bool
	ydb.IterateByIssues(err, func(_ string, code Ydb.StatusIds_StatusCode, _ uint32) {
		if code == ydbMissingTableIssueCode {
			b = true
		}
	})
	return b
}
