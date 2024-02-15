//go:build docker

package datastore

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
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
	e, cleanup, err := NewYDBEngineForTest(bridgeNetworkName)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	return e
}

// NewYDBEngineForTest returns a RunningEngineForTest for YDB.
func NewYDBEngineForTest(bridgeNetworkName string) (RunningEngineForTest, func(), error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, nil, err
	}

	ydbContainerOptions := &dockertest.RunOptions{
		Name:       fmt.Sprintf("ydb-%s", uuid.New().String()),
		Repository: "ghcr.io/ydb-platform/local-ydb",
		Tag:        ydbTestVersionTag,
		Env: []string{
			"YDB_USE_IN_MEMORY_PDISKS=true",
			"YDB_FEATURE_FLAGS=enable_not_null_data_columns",
		},
		NetworkID: bridgeNetworkName,
	}

	var ydbPort int

	// hostname must be resolvable from withing the network where tests are run.
	// we need to match hostPort with containerPort due to cluster discovery.
	if bridgeNetworkName != "" {
		ydbPort = ydbGRPCPort
		ydbContainerOptions.Hostname = ydbContainerOptions.Name
		ydbContainerOptions.PortBindings = nil
	} else {
		ydbPort, err = getFreePort()
		if err != nil {
			return nil, nil, err
		}

		ydbPortStr := strconv.FormatInt(int64(ydbPort), 10)
		ydbContainerOptions.Hostname = "localhost"
		ydbContainerOptions.PortBindings = map[docker.Port][]docker.PortBinding{
			docker.Port(ydbPortStr + "/tcp"): {{HostPort: ydbPortStr}},
		}
		ydbContainerOptions.Env = append(ydbContainerOptions.Env, "GRPC_PORT="+ydbPortStr)
		ydbContainerOptions.ExposedPorts = []string{ydbPortStr + "/tcp"}
	}

	resource, err := pool.RunWithOptions(ydbContainerOptions)
	if err != nil {
		return nil, nil, err
	}

	cleanup := func() {
		_ = pool.Purge(resource)
	}

	// await container is ready.
	// since YDB has internal cluster discovery we can't check availability from outside network.
	if err := pool.Retry(func() error {
		var buf bytes.Buffer

		code, err := resource.Exec([]string{
			"/ydb",
			"-e",
			fmt.Sprintf("grpc://localhost:%d", ydbPort),
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
	}); err != nil {
		cleanup()
		return nil, nil, err
	}

	return ydbTester{
		pool:              pool,
		bridgeNetworkName: bridgeNetworkName,
		hostname:          ydbContainerOptions.Hostname,
		port:              ydbPort,
	}, cleanup, nil
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

// getFreePort asks the kernel for a free open port that is ready to use.
func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}

	if err := l.Close(); err != nil {
		return 0, err
	}

	return l.Addr().(*net.TCPAddr).Port, nil
}
