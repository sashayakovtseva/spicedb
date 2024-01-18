package migrations

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"github.com/authzed/spicedb/pkg/migrate"
)

// YDBMigrations implements a migration manager for the YDBDriver.
var YDBMigrations = migrate.NewManager[*YDBDriver, table.Client, table.TransactionActor]()
