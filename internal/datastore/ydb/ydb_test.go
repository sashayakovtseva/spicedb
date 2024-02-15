//go:build ci && docker

package ydb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	log "github.com/authzed/spicedb/internal/logging"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/test"
)

var ydbTestEngine testdatastore.RunningEngineForTest

func TestMain(m *testing.M) {
	var (
		err     error
		cleanup func()
	)

	ydbTestEngine, cleanup, err = testdatastore.NewYDBEngineForTest("")
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create test ydb engine")
	}

	code := m.Run()
	cleanup()
	os.Exit(code)
}

func TestYDBDatastore(t *testing.T) {
	t.Skip("unimplemented datastore")

	b := testdatastore.RunYDBForTesting(t, "")
	test.All(t,
		test.DatastoreTesterFunc(func(
			revisionQuantization time.Duration,
			gcInterval time.Duration,
			gcWindow time.Duration,
			watchBufferLength uint16,
		) (datastore.Datastore, error) {
			ctx := context.Background()
			ds := b.NewDatastore(t, func(engine, dsn string) datastore.Datastore {
				ds, err := NewYDBDatastore(
					ctx,
					dsn,
				)
				require.NoError(t, err)
				return ds
			})

			return ds, nil
		}),
	)
}
