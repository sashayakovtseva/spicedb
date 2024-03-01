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
	test.AllWithExceptions(t,
		test.DatastoreTesterFunc(func(
			revisionQuantization time.Duration,
			gcInterval time.Duration,
			gcWindow time.Duration,
			watchBufferLength uint16,
		) (datastore.Datastore, error) {
			ctx := context.Background()
			ds := ydbTestEngine.NewDatastore(t, func(engine, dsn string) datastore.Datastore {
				ds, err := NewYDBDatastore(
					ctx,
					dsn,
					RevisionQuantization(revisionQuantization),
					GCInterval(gcInterval),
					GCWindow(gcWindow),
				)
				require.NoError(t, err)
				return ds
			})

			return ds, nil
		}),
		test.WithCategories(test.GCCategory, test.WatchCategory),
	)
}
