package ydb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestYDBReadWriterCaveats(t *testing.T) {
	ds := ydbTestEngine.NewDatastore(t, func(engine, dsn string) datastore.Datastore {
		ds, err := NewYDBDatastore(context.Background(), dsn)
		require.NoError(t, err)
		return ds
	})
	t.Cleanup(func() { ds.Close() })

	var writeRev datastore.Revision
	testCaveats := []*core.CaveatDefinition{
		0: ns.MustCaveatDefinition(
			caveats.MustEnvForVariables(map[string]caveattypes.VariableType{"v": caveattypes.IntType}),
			"one",
			"v == 1",
		),
		1: ns.MustCaveatDefinition(
			caveats.MustEnvForVariables(map[string]caveattypes.VariableType{"v": caveattypes.IntType}),
			"two",
			"v == 2",
		),
	}

	t.Run("initial write", func(t *testing.T) {
		var err error
		writeRev, err = ds.ReadWriteTx(
			context.Background(),
			func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
				return tx.WriteCaveats(ctx, testCaveats[:2])
			},
		)
		require.NoError(t, err)
	})
	require.NotNil(t, writeRev)

	t.Run("read written caveats", func(t *testing.T) {
		r := ds.SnapshotReader(writeRev)
		actual, err := r.ListAllCaveats(context.Background())
		require.NoError(t, err)
		require.ElementsMatch(t, []datastore.RevisionedCaveat{
			{
				LastWrittenRevision: writeRev,
				Definition:          testCaveats[0],
			},
			{
				LastWrittenRevision: writeRev,
				Definition:          testCaveats[1],
			},
		}, actual)
	})
}
