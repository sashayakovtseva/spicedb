package migrations

import (
	"context"
	"fmt"

	ydbOtel "github.com/ydb-platform/ydb-go-sdk-otel"
	ydbZerolog "github.com/ydb-platform/ydb-go-sdk-zerolog"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/migrate"
)

var _ migrate.Driver[table.Client, table.TransactionActor] = &YDBDriver{}

const (
	errUnableToInstantiate = "unable to instantiate YDBDriver: %w"

	queryLoadVersion  = "SELECT version_num from schema_version"
	queryWriteVersion = "UPDATE schema_version SET version_num=$newVersion WHERE version_num=$oldVersion"
)

// YDBDriver implements a schema migration facility for use in SpiceDB's YDB datastore.
type YDBDriver struct {
	db *ydb.Driver
}

// NewYDBDriver creates a new driver with active connections to the database specified.
func NewYDBDriver(ctx context.Context, url string) (*YDBDriver, error) {
	db, err := ydb.Open(ctx, url,
		ydbZerolog.WithTraces(&log.Logger, trace.DetailsAll),
		ydbOtel.WithTraces(),
	)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	return &YDBDriver{db}, nil
}

// Version returns the version of the schema to which the connected database has been migrated.
func (d *YDBDriver) Version(ctx context.Context) (string, error) {
	var loaded string

	if err := d.db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, res, err := s.Execute(ctx, table.DefaultTxControl(), queryLoadVersion, nil)
		if err != nil {
			return err
		}
		defer res.Close()

		if err := res.NextResultSetErr(ctx); err != nil {
			return err
		}

		for res.NextRow() {
			if err := res.Scan(&loaded); err != nil {
				return err
			}
		}
		return res.Err()
	}); err != nil {
		return "", fmt.Errorf("unable to load alembic revision: %w", err)
	}

	return loaded, nil
}

func (d *YDBDriver) WriteVersion(ctx context.Context, tx table.TransactionActor, version, replaced string) error {
	res, err := tx.Execute(ctx, queryWriteVersion,
		table.NewQueryParameters(
			table.ValueParam("$newVersion", types.TextValue(version)),
			table.ValueParam("$oldVersion", types.TextValue(replaced)),
		),
	)
	if err != nil {
		return fmt.Errorf("unable to update version row: %w", err)
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
