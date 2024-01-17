package migrations

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"github.com/authzed/spicedb/pkg/migrate"
)

var (
	noNonAtomicMigration migrate.MigrationFunc[table.Client]
	noAtomicMigration    migrate.TxMigrationFunc[table.TransactionActor]
)

// YDBMigrations implements a migration manager for the YDBDriver.
var YDBMigrations = migrate.NewManager[*YDBDriver, table.Client, table.TransactionActor]()
