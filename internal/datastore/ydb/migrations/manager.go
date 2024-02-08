package migrations

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"github.com/authzed/spicedb/pkg/migrate"
)

// YDBMigrations implements a migration manager for the YDBDriver.
var YDBMigrations = migrate.NewManager[*YDBDriver, TableClientWithOptions, TxActorWithOptions]()

type TableClientWithOptions struct {
	client table.Client
	opts   options
}

type TxActorWithOptions struct {
	tx   table.TransactionActor
	opts options
}
