//go:build ci && docker

package ydb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	datastoreCommon "github.com/authzed/spicedb/internal/datastore/common"
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

func TestYDBReadWriterNamespaces(t *testing.T) {
	ds := ydbTestEngine.NewDatastore(t, func(engine, dsn string) datastore.Datastore {
		ds, err := NewYDBDatastore(context.Background(), dsn)
		require.NoError(t, err)
		return ds
	})
	t.Cleanup(func() { ds.Close() })

	testNamespaces := []*core.NamespaceDefinition{
		0: ns.Namespace(
			"one",
			ns.MustRelation("somerel", nil),
		),
		1: ns.Namespace(
			"two",
		),
		2: ns.Namespace(
			"one",
			ns.MustRelation("updatedrel", nil),
		),
		3: ns.Namespace(
			"three",
		),
	}

	var initialRev datastore.Revision
	t.Run("initial write", func(t *testing.T) {
		var err error
		initialRev, err = ds.ReadWriteTx(
			context.Background(),
			func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
				return tx.WriteNamespaces(ctx, testNamespaces[:2]...)
			},
		)
		require.NoError(t, err)
	})
	require.NotNil(t, initialRev)

	t.Run("read written namespaces", func(t *testing.T) {
		r := ds.SnapshotReader(initialRev)
		actual, err := r.ListAllNamespaces(context.Background())
		require.NoError(t, err)
		require.Len(t, actual, 2)
		require.Equal(t, actual[0].LastWrittenRevision, initialRev)
		require.Equal(t, actual[1].LastWrittenRevision, initialRev)
		require.True(t, proto.Equal(actual[0].Definition, testNamespaces[0]))
		require.True(t, proto.Equal(actual[1].Definition, testNamespaces[1]))
	})

	var rewriteRev datastore.Revision
	t.Run("partial rewrite", func(t *testing.T) {
		var err error
		rewriteRev, err = ds.ReadWriteTx(
			context.Background(),
			func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
				return tx.WriteNamespaces(ctx, testNamespaces[2:4]...)
			},
		)
		require.NoError(t, err)
	})
	require.NotNil(t, rewriteRev)

	t.Run("read updated namespaces", func(t *testing.T) {
		r := ds.SnapshotReader(rewriteRev)
		actual, err := r.ListAllNamespaces(context.Background())
		require.NoError(t, err)
		require.Len(t, actual, 3)
		require.Equal(t, actual[0].LastWrittenRevision, rewriteRev)
		require.Equal(t, actual[1].LastWrittenRevision, rewriteRev)
		require.Equal(t, actual[2].LastWrittenRevision, initialRev)
		require.True(t, proto.Equal(actual[0].Definition, testNamespaces[2]))
		require.True(t, proto.Equal(actual[1].Definition, testNamespaces[3]))
		require.True(t, proto.Equal(actual[2].Definition, testNamespaces[1]))
	})

	var deleteRev datastore.Revision
	t.Run("delete namespaces", func(t *testing.T) {
		var err error
		deleteRev, err = ds.ReadWriteTx(
			context.Background(),
			func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
				return tx.DeleteNamespaces(ctx, "one", "two", "three")
			},
		)
		require.NoError(t, err)
	})
	require.NotNil(t, deleteRev)

	t.Run("read empty namespaces", func(t *testing.T) {
		r := ds.SnapshotReader(deleteRev)
		actual, err := r.ListAllNamespaces(context.Background())
		require.NoError(t, err)
		require.Empty(t, actual)
	})
}

func TestYDBReadWriterRelationships(t *testing.T) {
	ds := ydbTestEngine.NewDatastore(t, func(engine, dsn string) datastore.Datastore {
		ds, err := NewYDBDatastore(context.Background(), dsn)
		require.NoError(t, err)
		return ds
	})
	t.Cleanup(func() { ds.Close() })

	testRelations := []*core.RelationTuple{
		0: {
			ResourceAndRelation: &core.ObjectAndRelation{
				Namespace: "document",
				ObjectId:  "firstdoc",
				Relation:  "owner",
			},
			Subject: &core.ObjectAndRelation{
				Namespace: "user",
				ObjectId:  "bob",
			},
			Caveat: &core.ContextualizedCaveat{
				CaveatName: "on_weekend",
				Context: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"foo": structpb.NewBoolValue(true),
					},
				},
			},
		},
		1: {
			ResourceAndRelation: &core.ObjectAndRelation{
				Namespace: "document",
				ObjectId:  "firstdoc",
				Relation:  "reader",
			},
			Subject: &core.ObjectAndRelation{
				Namespace: "user",
				ObjectId:  "fred",
			},
		},
		2: {
			ResourceAndRelation: &core.ObjectAndRelation{
				Namespace: "document",
				ObjectId:  "seconddoc",
				Relation:  "reader",
			},
			Subject: &core.ObjectAndRelation{
				Namespace: "user",
				ObjectId:  "bob",
			},
		},
		3: {
			ResourceAndRelation: &core.ObjectAndRelation{
				Namespace: "document",
				ObjectId:  "firstdoc",
				Relation:  "owner",
			},
			Subject: &core.ObjectAndRelation{
				Namespace: "user",
				ObjectId:  "bob",
			},
			Caveat: &core.ContextualizedCaveat{
				CaveatName: "with_ip",
				Context: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"bar": structpb.NewBoolValue(false),
					},
				},
			},
		},
		4: {
			ResourceAndRelation: &core.ObjectAndRelation{
				Namespace: "document",
				ObjectId:  "seconddoc",
				Relation:  "reader",
			},
			Subject: &core.ObjectAndRelation{
				Namespace: "user",
				ObjectId:  "alice",
			},
		},
	}

	testQueryRelationships := func(
		t *testing.T,
		rev datastore.Revision,
		filter datastore.RelationshipsFilter,
		expect []*core.RelationTuple,
	) {
		r := ds.SnapshotReader(rev)
		it, err := r.QueryRelationships(context.Background(), filter)
		require.NoError(t, err)
		t.Cleanup(it.Close)

		var i int
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			require.True(t, proto.Equal(tpl, expect[i]))
			i++
		}
		require.Equal(t, len(expect), i)
	}

	var initialRev datastore.Revision
	t.Run("initial write", func(t *testing.T) {
		var err error
		initialRev, err = ds.ReadWriteTx(
			context.Background(),
			func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
				return tx.WriteRelationships(ctx, []*core.RelationTupleUpdate{
					{
						Operation: core.RelationTupleUpdate_CREATE,
						Tuple:     testRelations[0],
					},
					{
						Operation: core.RelationTupleUpdate_CREATE,
						Tuple:     testRelations[1],
					},
				})
			},
		)
		require.NoError(t, err)
	})
	require.NotNil(t, initialRev)

	t.Run("read written relationships", func(t *testing.T) {
		testQueryRelationships(t, initialRev, datastore.RelationshipsFilter{
			ResourceType:        "document",
			OptionalResourceIds: []string{"firstdoc"},
		}, testRelations[:2])
	})

	t.Run("ensure duplicate check works", func(t *testing.T) {
		rev, err := ds.ReadWriteTx(
			context.Background(),
			func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
				return tx.WriteRelationships(ctx, []*core.RelationTupleUpdate{
					{
						Operation: core.RelationTupleUpdate_CREATE,
						Tuple:     testRelations[0],
					},
				})
			},
		)
		var dsErr datastoreCommon.CreateRelationshipExistsError
		require.ErrorAs(t, err, &dsErr)
		require.True(t, proto.Equal(dsErr.Relationship, testRelations[0]))
		require.Equal(t, datastore.NoRevision, rev)
	})

	var deleteRev datastore.Revision
	t.Run("delete relationship", func(t *testing.T) {
		var err error
		deleteRev, err = ds.ReadWriteTx(
			context.Background(),
			func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
				return tx.WriteRelationships(ctx, []*core.RelationTupleUpdate{
					{
						Operation: core.RelationTupleUpdate_DELETE,
						Tuple:     testRelations[1],
					},
				})
			},
		)
		require.NoError(t, err)
	})
	require.NotNil(t, deleteRev)

	t.Run("read relationships after delete", func(t *testing.T) {
		testQueryRelationships(t, deleteRev, datastore.RelationshipsFilter{
			ResourceType:        "document",
			OptionalResourceIds: []string{"firstdoc"},
		}, testRelations[:1])
	})

	t.Run("ensure snapshot read sees deleted relationships", func(t *testing.T) {
		testQueryRelationships(t, initialRev, datastore.RelationshipsFilter{
			ResourceType:        "document",
			OptionalResourceIds: []string{"firstdoc"},
		}, testRelations[:2])
	})

	var touchRev datastore.Revision
	t.Run("touch operation", func(t *testing.T) {
		var err error
		touchRev, err = ds.ReadWriteTx(
			context.Background(),
			func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
				return tx.WriteRelationships(ctx, []*core.RelationTupleUpdate{
					{
						Operation: core.RelationTupleUpdate_TOUCH,
						Tuple:     testRelations[0],
					},
					{
						Operation: core.RelationTupleUpdate_TOUCH,
						Tuple:     testRelations[2],
					},
				})
			},
		)
		require.NoError(t, err)
	})
	require.NotNil(t, touchRev)

	t.Run("read relationships after touch", func(t *testing.T) {
		testQueryRelationships(t, touchRev, datastore.RelationshipsFilter{
			ResourceType: "document",
		}, []*core.RelationTuple{testRelations[0], testRelations[2]})
	})

	var touchCaveatRev datastore.Revision
	t.Run("touch operation with caveat update", func(t *testing.T) {
		var err error
		touchCaveatRev, err = ds.ReadWriteTx(
			context.Background(),
			func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
				return tx.WriteRelationships(ctx, []*core.RelationTupleUpdate{
					{
						Operation: core.RelationTupleUpdate_TOUCH,
						Tuple:     testRelations[2],
					},
					{
						Operation: core.RelationTupleUpdate_TOUCH,
						Tuple:     testRelations[3],
					},
				})
			},
		)
		require.NoError(t, err)
	})
	require.NotNil(t, touchCaveatRev)

	t.Run("read relationships after touch", func(t *testing.T) {
		testQueryRelationships(t, touchCaveatRev, datastore.RelationshipsFilter{
			ResourceType: "document",
		}, []*core.RelationTuple{testRelations[3], testRelations[2]})
	})

	var pkErrRev datastore.Revision
	t.Run("ensure pk duplicates are not possible", func(t *testing.T) {
		var err error
		pkErrRev, err = ds.ReadWriteTx(
			context.Background(),
			func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
				return tx.WriteRelationships(ctx, []*core.RelationTupleUpdate{
					{
						Operation: core.RelationTupleUpdate_TOUCH,
						Tuple:     testRelations[4],
					},
					{
						Operation: core.RelationTupleUpdate_CREATE,
						Tuple:     testRelations[4],
					},
				})
			},
		)
		require.Error(t, err)
	})
	require.Equal(t, datastore.NoRevision, pkErrRev)
}
