//go:build ci && docker

package ydb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/test"
)

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
