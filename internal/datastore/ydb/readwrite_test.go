package ydb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

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
		2: ns.MustCaveatDefinition(
			caveats.MustEnvForVariables(map[string]caveattypes.VariableType{"v": caveattypes.DoubleType}),
			"one",
			"v == 1.1",
		),
		3: ns.MustCaveatDefinition(
			caveats.MustEnvForVariables(map[string]caveattypes.VariableType{"v": caveattypes.IntType}),
			"three",
			"v == 3",
		),
	}

	var initialRev datastore.Revision
	t.Run("initial write", func(t *testing.T) {
		var err error
		initialRev, err = ds.ReadWriteTx(
			context.Background(),
			func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
				return tx.WriteCaveats(ctx, testCaveats[:2])
			},
		)
		require.NoError(t, err)
	})
	require.NotNil(t, initialRev)

	t.Run("read written caveats", func(t *testing.T) {
		r := ds.SnapshotReader(initialRev)
		actual, err := r.ListAllCaveats(context.Background())
		require.NoError(t, err)
		require.Len(t, actual, 2)
		require.Equal(t, actual[0].LastWrittenRevision, initialRev)
		require.Equal(t, actual[1].LastWrittenRevision, initialRev)
		require.True(t, proto.Equal(actual[0].Definition, testCaveats[0]))
		require.True(t, proto.Equal(actual[1].Definition, testCaveats[1]))
	})

	var rewriteRev datastore.Revision
	t.Run("partial rewrite", func(t *testing.T) {
		var err error
		rewriteRev, err = ds.ReadWriteTx(
			context.Background(),
			func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
				return tx.WriteCaveats(ctx, testCaveats[2:4])
			},
		)
		require.NoError(t, err)
	})
	require.NotNil(t, rewriteRev)

	t.Run("read updated caveats", func(t *testing.T) {
		r := ds.SnapshotReader(rewriteRev)
		actual, err := r.ListAllCaveats(context.Background())
		require.NoError(t, err)
		require.Len(t, actual, 3)
		require.Equal(t, actual[0].LastWrittenRevision, rewriteRev)
		require.Equal(t, actual[1].LastWrittenRevision, rewriteRev)
		require.Equal(t, actual[2].LastWrittenRevision, initialRev)
		require.True(t, proto.Equal(actual[0].Definition, testCaveats[2]))
		require.True(t, proto.Equal(actual[1].Definition, testCaveats[3]))
		require.True(t, proto.Equal(actual[2].Definition, testCaveats[1]))
	})

	var deleteRev datastore.Revision
	t.Run("delete caveats", func(t *testing.T) {
		var err error
		deleteRev, err = ds.ReadWriteTx(
			context.Background(),
			func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
				return tx.DeleteCaveats(ctx, []string{"one", "two", "three"})
			},
		)
		require.NoError(t, err)
	})
	require.NotNil(t, deleteRev)

	t.Run("read empty caveats", func(t *testing.T) {
		r := ds.SnapshotReader(deleteRev)
		actual, err := r.ListAllCaveats(context.Background())
		require.NoError(t, err)
		require.Empty(t, actual)
	})
}
