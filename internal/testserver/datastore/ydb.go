//go:build docker

package datastore

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"

	ydbMigrations "github.com/authzed/spicedb/internal/datastore/ydb/migrations"
	"github.com/authzed/spicedb/pkg/secrets"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
)

const (
	ydbTestVersionTag  = "nightly"
	ydbDefaultDatabase = "local"
	ydbGRPCPort        = 2136
)

type ydbTester struct {
	pool              *dockertest.Pool
	bridgeNetworkName string

	hostname string
	port     int
}

// RunYDBForTesting returns a RunningEngineForTest for YDB.
func RunYDBForTesting(t testing.TB, bridgeNetworkName string) RunningEngineForTest {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	ydbContainerOptions := &dockertest.RunOptions{
		Name: fmt.Sprintf("ydb-%s", uuid.New().String()),
		// hostname must be resolvable from withing the network where tests are run.
		Hostname:   "localhost",
		Repository: "ghcr.io/ydb-platform/local-ydb",
		Tag:        ydbTestVersionTag,
		Env: []string{
			"YDB_USE_IN_MEMORY_PDISKS=true",
			"YDB_FEATURE_FLAGS=enable_not_null_data_columns",
		},
		// we need to match hostPort with containerPort due to cluster discovery.
		PortBindings: map[docker.Port][]docker.PortBinding{
			"2136/tcp": {{HostPort: "2136"}},
		},
		NetworkID: bridgeNetworkName,
	}

	if bridgeNetworkName != "" {
		ydbContainerOptions.Hostname = ydbContainerOptions.Name
		ydbContainerOptions.PortBindings = nil
	}

	resource, err := pool.RunWithOptions(ydbContainerOptions)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, pool.Purge(resource))
	})

	// await container is ready.
	// since YDB has internal cluster discovery we can't check availability from outside network.
	require.NoError(t, pool.Retry(func() error {
		var buf bytes.Buffer

		code, err := resource.Exec([]string{
			"/ydb",
			"-e",
			fmt.Sprintf("grpc://localhost:%d", ydbGRPCPort),
			"-d",
			"/" + ydbDefaultDatabase,
			"scheme",
			"ls",
		}, dockertest.ExecOptions{
			StdErr: &buf,
		})
		if err != nil {
			return fmt.Errorf("%w: %s", err, buf.String())
		}
		if code != 0 {
			return fmt.Errorf("exited with %d: %s", code, buf.String())
		}

		return nil
	}))

	return ydbTester{
		pool:              pool,
		bridgeNetworkName: bridgeNetworkName,
		hostname:          ydbContainerOptions.Hostname,
		port:              ydbGRPCPort,
	}
}

func (r ydbTester) NewDatabase(t testing.TB) string {
	// there's no easy way to create new database in a local YDB,
	// so create a new directory instead.

	uniquePortion, err := secrets.TokenHex(4)
	require.NoError(t, err)

	directory := fmt.Sprintf("/%s/%s", ydbDefaultDatabase, uniquePortion)
	dsn := fmt.Sprintf("grpc://%s:%d/%s?table_path_prefix=%s", r.hostname, r.port, ydbDefaultDatabase, directory)

	return dsn
}

func (r ydbTester) NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore {
	dsn := r.NewDatabase(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	migrationDriver, err := ydbMigrations.NewYDBDriver(ctx, dsn)
	require.NoError(t, err)

	err = ydbMigrations.YDBMigrations.Run(ctx, migrationDriver, migrate.Head, migrate.LiveRun)
	require.NoError(t, err)

	return initFunc("ydb", dsn)
}
