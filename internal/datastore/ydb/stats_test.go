package ydb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	testserverDatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
)

func TestYDBDatastoreStatistics(t *testing.T) {
	engine := testserverDatastore.RunYDBForTesting(t, "")

	ds := engine.NewDatastore(t, func(engine, dsn string) datastore.Datastore {
		ds, err := NewYDBDatastore(
			context.Background(),
			dsn,
		)
		require.NoError(t, err)
		return ds
	})

	stats, err := ds.Statistics(context.Background())
	require.NoError(t, err)

	require.NotEmpty(t, stats.UniqueID)
	t.Logf("unique_id: %s", stats.UniqueID)
}
