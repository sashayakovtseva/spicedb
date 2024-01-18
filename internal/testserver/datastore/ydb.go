//go:build docker

package datastore

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3"

	ydbDatastore "github.com/authzed/spicedb/internal/datastore/ydb"
	ydbMigrations "github.com/authzed/spicedb/internal/datastore/ydb/migrations"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
)

const (
	ydbTestVersionTag  = "23.3"
	ydbDefaultDatabase = "local"
	ydbGRPCPort        = 2136
)

type ydbTester struct {
	pool              *dockertest.Pool
	bridgeNetworkName string

	hostname string
	port     string
	dsn      string
}

// RunYDBForTesting returns a RunningEngineForTest for YDB.
func RunYDBForTesting(t testing.TB, bridgeNetworkName string) RunningEngineForTest {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	return ydbTester{
		pool:              pool,
		bridgeNetworkName: bridgeNetworkName,
	}
}

func (r ydbTester) NewDatabase(t testing.TB) string {
	// there's no easy way to create new database in a local YDB, so
	// create a new container with default /local database instead.

	containerName := fmt.Sprintf("ydb-%s", uuid.New().String())
	resource, err := r.pool.RunWithOptions(&dockertest.RunOptions{
		Name:       containerName,
		Hostname:   "localhost",
		Repository: "cr.yandex/yc/yandex-docker-local-ydb",
		Tag:        ydbTestVersionTag,
		Env:        []string{"YDB_USE_IN_MEMORY_PDISKS=true"},
		NetworkID:  r.bridgeNetworkName,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.pool.Purge(resource)) })

	hostname := "localhost"
	port := resource.GetPort(fmt.Sprintf("%d/tcp", ydbGRPCPort))
	if r.bridgeNetworkName != "" {
		hostname = containerName
		port = strconv.FormatInt(ydbGRPCPort, 10)
	}

	dsn := fmt.Sprintf("grpc://%s:%s/%s", hostname, port, ydbDefaultDatabase)
	require.NoError(t, r.pool.Retry(func() error { // await container is ready
		ctx, cancel := context.WithTimeout(context.Background(), dockerBootTimeout)
		defer cancel()

		driver, err := ydb.Open(ctx, dsn)
		if err != nil {
			return err
		}

		if _, err := driver.Scheme().ListDirectory(ctx, ydbDefaultDatabase); err != nil {
			return err
		}

		return nil
	}))
	return dsn
}

func (r ydbTester) NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore {
	dsn := r.NewDatabase(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	migrationDriver, err := ydbMigrations.NewYDBDriver(ctx, dsn)
	require.NoError(t, err)

	err = ydbMigrations.YDBMigrations.Run(ctx, migrationDriver, migrate.Head, migrate.LiveRun)
	require.NoError(t, err)

	return initFunc(ydbDatastore.Engine, dsn)
}
