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

var _ migrate.Driver[TableClientWithConfig, TxActorWithConfig] = (*YDBDriver)(nil)

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

// YDBDriver implements a schema migration facility for use in SpiceDB's YDB datastore.
type YDBDriver struct {
	db     *ydb.Driver
	config *ydbConfig
}

// NewYDBDriver creates a new driver with active connections to the database specified.
func NewYDBDriver(ctx context.Context, dsn string, opts ...Option) (*YDBDriver, error) {
	parsedDSN := common.ParseDSN(dsn)

	config := generateConfig(opts)
	if parsedDSN.TablePathPrefix != "" {
		config.tablePathPrefix = parsedDSN.TablePathPrefix
	}

	ydbOpts := []ydb.Option{
		ydbZerolog.WithTraces(&log.Logger, trace.DatabaseSQLEvents),
		ydbOtel.WithTraces(),
	}
	if config.certificatePath != "" {
		ydbOpts = append(ydbOpts, ydb.WithCertificatesFromFile(config.certificatePath))
	}

	db, err := ydb.Open(ctx, parsedDSN.OriginalDSN, ydbOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate YDBDriver: %w", err)
	}

	if _, err := db.Scheme().ListDirectory(ctx, db.Name()); err != nil {
		return nil, fmt.Errorf("failed to ping YDB: %w", err)
	}

	return &YDBDriver{
		db:     db,
		config: config,
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
			common.AddTablePrefix(queryLoadVersion, d.config.tablePathPrefix),
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

func (d *YDBDriver) WriteVersion(ctx context.Context, tx TxActorWithConfig, version, _ string) error {
	res, err := tx.tx.Execute(
		ctx,
		common.AddTablePrefix(queryWriteVersion, tx.config.tablePathPrefix),
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
func (d *YDBDriver) Conn() TableClientWithConfig {
	return TableClientWithConfig{
		client: d.db.Table(),
		config: d.config,
	}
}

func (d *YDBDriver) RunTx(ctx context.Context, f migrate.TxMigrationFunc[TxActorWithConfig]) error {
	return d.db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		return f(ctx, TxActorWithConfig{
			tx:     tx,
			config: d.config,
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
