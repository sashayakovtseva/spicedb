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

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/truetime"
)

var _ migrate.Driver[table.Client, table.TransactionActor] = (*YDBDriver)(nil)

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
	db *ydb.Driver
}

// NewYDBDriver creates a new driver with active connections to the database specified.
func NewYDBDriver(ctx context.Context, dsn string) (*YDBDriver, error) {
	db, err := ydb.Open(ctx, dsn,
		ydbZerolog.WithTraces(&log.Logger, trace.DatabaseSQLEvents),
		ydbOtel.WithTraces(),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to instantiate YDBDriver: %w", err)
	}

	return &YDBDriver{db}, nil
}

// Version returns the current version of the schema in the backing datastore.
// If the datastore is brand new, version returns the empty string without an error.
func (d *YDBDriver) Version(ctx context.Context) (string, error) {
	var (
		loaded            string
		createdAtUnixNano *int64 // don't really need this
	)

	err := d.db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, res, err := s.Execute(ctx, table.DefaultTxControl(), queryLoadVersion, nil)
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
		return "", fmt.Errorf("unable to load alembic revision: %w", err)
	}

	return loaded, nil
}

func (d *YDBDriver) WriteVersion(ctx context.Context, tx table.TransactionActor, version, replaced string) error {
	res, err := tx.Execute(ctx, queryWriteVersion,
		table.NewQueryParameters(
			table.ValueParam("$newVersion", types.TextValue(version)),
			table.ValueParam("$createAtUnixNano", types.Int64Value(truetime.UnixNano())),
		),
	)
	if err != nil {
		return fmt.Errorf("unable to insert new version row: %w", err)
	}
	defer res.Close()

	return res.Err()
}

// Conn returns the underlying table client instance for this driver.
func (d *YDBDriver) Conn() table.Client {
	return d.db.Table()
}

func (d *YDBDriver) RunTx(ctx context.Context, f migrate.TxMigrationFunc[table.TransactionActor]) error {
	return d.db.Table().DoTx(ctx, table.TxOperation(f))
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
