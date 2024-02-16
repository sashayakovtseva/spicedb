package ydb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/internal/datastore/ydb/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestYDBReaderNamespaces(t *testing.T) {
	ds := ydbTestEngine.NewDatastore(t, func(engine, dsn string) datastore.Datastore {
		ds, err := NewYDBDatastore(context.Background(), dsn)
		require.NoError(t, err)

		yDS, err := newYDBDatastore(context.Background(), dsn)
		require.NoError(t, err)
		t.Cleanup(func() { yDS.Close() })

		err = yDS.driver.Table().Do(context.Background(), func(ctx context.Context, s table.Session) error {
			testNS := []struct {
				name         string
				configBase64 string
				createdAt    int64
				deletedAt    *int64
			}{
				{
					name: "user",
					configBase64: "CgR1c2VyEmEKBmZyaWVuZBoTChEKBHVzZXIqBAgBEBMaAy4uLiI0CjIKLHR5cGUuZ29vZ2" +
						"xlYXBpcy5jb20vaW1wbC52MS5SZWxhdGlvbk1ldGFkYXRhEgIIASoECAEQAjoGZnJpZW5kIgA=",
					createdAt: 1,
					deletedAt: proto.Int64(7),
				},
				{
					name: "document",
					configBase64: "Cghkb2N1bWVudBJtCgxjb2xsYWJvcmF0b3IaEwoRCgR1c2VyKgQIBRAZGgMuLi4iNAoyCi" +
						"x0eXBlLmdvb2dsZWFwaXMuY29tL2ltcGwudjEuUmVsYXRpb25NZXRhZGF0YRICCAEqBAgFEAI6DGNvbG" +
						"xhYm9yYXRvciICCAQ=",
					createdAt: 1,
					deletedAt: proto.Int64(7),
				},
				{
					name:         "user",
					configBase64: "CgR1c2Vy",
					createdAt:    10,
				},
				{
					name: "document",
					configBase64: "Cghkb2N1bWVudBJhCgZ3cml0ZXIaEwoRCgR1c2VyKgQIAxATGgMuLi4iNAoyCix0eXBlLmdvb2ds" +
						"ZWFwaXMuY29tL2ltcGwudjEuUmVsYXRpb25NZXRhZGF0YRICCAEqBAgDEAI6BndyaXRlchJhCgZy" +
						"ZWFkZXIaEwoRCgR1c2VyKgQIBBATGgMuLi4iNAoyCix0eXBlLmdvb2dsZWFwaXMuY29tL2ltcGwu" +
						"djEuUmVsYXRpb25NZXRhZGF0YRICCAEqBAgEEAI6BnJlYWRlchJ5CgRlZGl0EhoiBAgGEBQKEgoQ" +
						"KgQIBhAUEggSBndyaXRlciI0CjIKLHR5cGUuZ29vZ2xlYXBpcy5jb20vaW1wbC52MS5SZWxhdGlv" +
						"bk1ldGFkYXRhEgIIAioECAYQAjIGd3JpdGVyOhElNTk2YTg2NjBmOWEwYzA4NRKBAQoEdmlldxIq" +
						"IgQIBxAUCiIKECoECAcQFBIIEgZyZWFkZXIKDioECAcQHRIGEgRlZGl0IjQKMgosdHlwZS5nb29n" +
						"bGVhcGlzLmNvbS9pbXBsLnYxLlJlbGF0aW9uTWV0YWRhdGESAggCKgQIBxACOhElMGNiNTFkYTIw" +
						"ZmM5ZjIwZiICCAI=",
					createdAt: 10,
				},
			}

			stmt, err := s.Prepare(
				ctx,
				common.AddTablePrefix(`
					DECLARE $name AS Utf8;
					DECLARE $config AS String;
					DECLARE $createdAt AS Int64;
					DECLARE $deletedAt AS Optional<Int64>;
					INSERT INTO
						namespace_config(namespace, serialized_config, created_at_unix_nano, deleted_at_unix_nano)
					VALUES 
						($name, $config, $createdAt, $deletedAt);
				`, yDS.config.tablePathPrefix),
			)
			if err != nil {
				return err
			}

			for i := 0; i < len(testNS); i++ {
				conf, err := base64.StdEncoding.DecodeString(testNS[i].configBase64)
				if err != nil {
					return err
				}

				_, _, err = stmt.Execute(
					ctx,
					table.DefaultTxControl(),
					table.NewQueryParameters(
						table.ValueParam("$name", types.UTF8Value(testNS[i].name)),
						table.ValueParam("$config", types.BytesValue(conf)),
						table.ValueParam("$createdAt", types.Int64Value(testNS[i].createdAt)),
						table.ValueParam("$deletedAt", types.NullableInt64Value(testNS[i].deletedAt)),
					),
				)
				if err != nil {
					return err
				}
			}

			return nil
		})
		require.NoError(t, err)

		return ds
	})
	t.Cleanup(func() { ds.Close() })

	type expectNs struct {
		name                string
		relations           []string
		lastWrittenRevision datastore.Revision
	}

	matchNamespace := func(t *testing.T, expect expectNs, actual datastore.RevisionedNamespace) {
		require.Equal(t, expect.lastWrittenRevision, actual.LastWrittenRevision)
		require.Equal(t, expect.name, actual.Definition.GetName())
		actualRelations := lo.Map(actual.Definition.GetRelation(), func(item *core.Relation, index int) string {
			return item.GetName()
		})
		require.ElementsMatch(t, expect.relations, actualRelations)
	}

	testListAllNamespaces := func(t *testing.T, r datastore.Reader, expectNs map[string]expectNs) {
		nss, err := r.ListAllNamespaces(context.Background())
		require.NoError(t, err)
		require.Len(t, nss, len(expectNs))
		for _, ns := range nss {
			expectNs, ok := expectNs[ns.Definition.GetName()]
			require.True(t, ok)
			matchNamespace(t, expectNs, ns)
		}
	}

	testLookupNamespacesWithNames := func(
		t *testing.T,
		r datastore.Reader,
		in []string,
		expect map[string]expectNs,
	) {
		nss, err := r.LookupNamespacesWithNames(context.Background(), in)
		require.NoError(t, err)
		require.Len(t, nss, len(expect))
		for _, ns := range nss {
			expectNs, ok := expect[ns.Definition.GetName()]
			require.True(t, ok)
			matchNamespace(t, expectNs, ns)
		}
	}

	testReadNamespaceByName := func(t *testing.T, r datastore.Reader, expect expectNs) {
		ns, lastWritten, err := r.ReadNamespaceByName(context.Background(), expect.name)
		if expect.name == "" {
			require.ErrorAs(t, err, new(datastore.ErrNotFound))
			return
		}
		require.NoError(t, err)
		matchNamespace(t, expect, datastore.RevisionedNamespace{
			Definition:          ns,
			LastWrittenRevision: lastWritten,
		})
	}

	t.Run("removed namespaces not garbage collected", func(t *testing.T) {
		testRevision := revisions.NewForTimestamp(6)
		expectedNs := map[string]expectNs{
			"user": {
				name:                "user",
				relations:           []string{"friend"},
				lastWrittenRevision: revisions.NewForTimestamp(1),
			},
			"document": {
				name:                "document",
				relations:           []string{"collaborator"},
				lastWrittenRevision: revisions.NewForTimestamp(1),
			},
		}
		r := ds.SnapshotReader(testRevision)

		testListAllNamespaces(t, r, expectedNs)
		testReadNamespaceByName(t, r, expectedNs["user"])
		testLookupNamespacesWithNames(t, r,
			[]string{"user", "unknown"}, map[string]expectNs{"user": expectedNs["user"]})
	})

	t.Run("latest namespaces", func(t *testing.T) {
		testRevision := revisions.NewForTimestamp(10)
		expectedNs := map[string]expectNs{
			"user": {
				name:                "user",
				lastWrittenRevision: revisions.NewForTimestamp(10),
			},
			"document": {
				name:                "document",
				relations:           []string{"writer", "reader", "edit", "view"},
				lastWrittenRevision: revisions.NewForTimestamp(10),
			},
		}
		r := ds.SnapshotReader(testRevision)
		testListAllNamespaces(t, r, expectedNs)
		testReadNamespaceByName(t, r, expectedNs["document"])
		testLookupNamespacesWithNames(t, r,
			[]string{"document", "unknown"}, map[string]expectNs{"document": expectedNs["document"]})
	})

	t.Run("no namespaces", func(t *testing.T) {
		testRevision := revisions.NewForTimestamp(7)
		expectedNs := map[string]expectNs{}
		r := ds.SnapshotReader(testRevision)
		testListAllNamespaces(t, r, expectedNs)
		testReadNamespaceByName(t, r, expectedNs["user"])
		testLookupNamespacesWithNames(t, r, []string{"user", "document"}, nil)
	})
}

func TestYDBReaderCaveats(t *testing.T) {
	ds := ydbTestEngine.NewDatastore(t, func(engine, dsn string) datastore.Datastore {
		ds, err := NewYDBDatastore(context.Background(), dsn)
		require.NoError(t, err)

		yDS, err := newYDBDatastore(context.Background(), dsn)
		require.NoError(t, err)
		t.Cleanup(func() { yDS.Close() })

		err = yDS.driver.Table().Do(context.Background(), func(ctx context.Context, s table.Session) error {
			testCaveat := []struct {
				name         string
				configBase64 string
				createdAt    int64
				deletedAt    *int64
			}{
				{
					name: "one",
					configBase64: "CgNvbmUScRIDb25lCmoSDAgCEggaBmVxdWFscxIHCAESAwoBdhoGCAMSAhgCGgYIAhICGAEaBggB" +
						"EgIYAiIbEAIyFxIEXz09XxoHEAEiAwoBdhoGEAMaAhgBKhwSA29uZRoDFB0eIgQIAhAYIgQIAxAb" +
						"IgQIARAWGgoKAXYSBQoDaW50KgA=",
					createdAt: 1,
					deletedAt: proto.Int64(7),
				},
				{
					name: "two",
					configBase64: "CgN0d28SdBIDdHdvCm0SBwgBEgMKAXYSDAgCEggaBmVxdWFscxoGCAMSAhgCGgYIAhICGAEaBggB" +
						"EgIYAiIbEAIyFxIEXz09XxoHEAEiAwoBdhoGEAMaAhgCKh8SA3R3bxoGMDEyMzw9IgQIARA1IgQI" +
						"AhA3IgQIAxA6GgoKAXYSBQoDaW50KgIIAw==",
					createdAt: 1,
					deletedAt: proto.Int64(7),
				},
				{
					name: "three",
					configBase64: "CgV0aHJlZRJ7EgV0aHJlZQpyEgcIARIDCgF2EgwIAhIIGgZlcXVhbHMaBggDEgIYAhoGCAISAhgB" +
						"GgYIARICGAIiGxACMhcSBF89PV8aBxABIgMKAXYaBhADGgIYAyokEgV0aHJlZRoJTk9QUVJTVF1e" +
						"IgQIARBWIgQIAhBYIgQIAxBbGgoKAXYSBQoDaW50KgIIBg==",
					createdAt: 10,
				},
				{
					name: "four",
					configBase64: "CgRmb3VyEnwSBGZvdXIKdBIHCAESAwoBdhIMCAISCBoGZXF1YWxzGgYIARICGAIaBggDEgIYAhoG" +
						"CAISAhgBIhsQAjIXEgRfPT1fGgcQASIDCgF2GgYQAxoCGAQqJhIEZm91choMa2xtbm9wcXJzdH1+" +
						"IgQIARB2IgQIAhB4IgQIAxB7GgoKAXYSBQoDaW50KgIICQ==",
					createdAt: 10,
				},
			}

			stmt, err := s.Prepare(
				ctx,
				common.AddTablePrefix(`
					DECLARE $name AS Utf8;
					DECLARE $config AS String;
					DECLARE $createdAt AS Int64;
					DECLARE $deletedAt AS Optional<Int64>;
					INSERT INTO
						caveat(name, definition, created_at_unix_nano, deleted_at_unix_nano)
					VALUES 
						($name, $config, $createdAt, $deletedAt);
				`, yDS.config.tablePathPrefix),
			)
			if err != nil {
				return err
			}

			for i := 0; i < len(testCaveat); i++ {
				conf, err := base64.StdEncoding.DecodeString(testCaveat[i].configBase64)
				if err != nil {
					return err
				}

				_, _, err = stmt.Execute(
					ctx,
					table.DefaultTxControl(),
					table.NewQueryParameters(
						table.ValueParam("$name", types.UTF8Value(testCaveat[i].name)),
						table.ValueParam("$config", types.BytesValue(conf)),
						table.ValueParam("$createdAt", types.Int64Value(testCaveat[i].createdAt)),
						table.ValueParam("$deletedAt", types.NullableInt64Value(testCaveat[i].deletedAt)),
					),
				)
				if err != nil {
					return err
				}
			}

			return nil
		})
		require.NoError(t, err)

		return ds
	})
	t.Cleanup(func() { ds.Close() })

	type expectCaveat struct {
		name                string
		lastWrittenRevision datastore.Revision
	}

	matchCaveat := func(t *testing.T, expect expectCaveat, actual datastore.RevisionedCaveat) {
		require.Equal(t, expect.lastWrittenRevision, actual.LastWrittenRevision)
		require.Equal(t, expect.name, actual.Definition.GetName())
	}

	testListAllCaveats := func(t *testing.T, r datastore.Reader, expectCaveat map[string]expectCaveat) {
		vv, err := r.ListAllCaveats(context.Background())
		require.NoError(t, err)
		require.Len(t, vv, len(expectCaveat))
		for _, v := range vv {
			expectCaveat, ok := expectCaveat[v.Definition.GetName()]
			require.True(t, ok)
			matchCaveat(t, expectCaveat, v)
		}
	}

	testLookupCaveatsWithNames := func(
		t *testing.T,
		r datastore.Reader,
		in []string,
		expect map[string]expectCaveat,
	) {
		vv, err := r.LookupCaveatsWithNames(context.Background(), in)
		require.NoError(t, err)
		require.Len(t, vv, len(expect))
		for _, v := range vv {
			expectCaveat, ok := expect[v.Definition.GetName()]
			require.True(t, ok)
			matchCaveat(t, expectCaveat, v)
		}
	}

	testReadCaveatByName := func(t *testing.T, r datastore.Reader, expect expectCaveat) {
		v, lastWritten, err := r.ReadCaveatByName(context.Background(), expect.name)
		if expect.name == "" {
			require.ErrorAs(t, err, new(datastore.ErrNotFound))
			return
		}
		require.NoError(t, err)
		matchCaveat(t, expect, datastore.RevisionedCaveat{
			Definition:          v,
			LastWrittenRevision: lastWritten,
		})
	}

	t.Run("removed caveats not garbage collected", func(t *testing.T) {
		testRevision := revisions.NewForTimestamp(6)
		expectedNs := map[string]expectCaveat{
			"one": {
				name:                "one",
				lastWrittenRevision: revisions.NewForTimestamp(1),
			},
			"two": {
				name:                "two",
				lastWrittenRevision: revisions.NewForTimestamp(1),
			},
		}
		r := ds.SnapshotReader(testRevision)

		testListAllCaveats(t, r, expectedNs)
		testReadCaveatByName(t, r, expectedNs["onw"])
		testLookupCaveatsWithNames(t, r,
			[]string{"one", "unknown"},
			map[string]expectCaveat{"one": expectedNs["one"]},
		)
	})

	t.Run("latest caveats", func(t *testing.T) {
		testRevision := revisions.NewForTimestamp(10)
		expectedNs := map[string]expectCaveat{
			"three": {
				name:                "three",
				lastWrittenRevision: revisions.NewForTimestamp(10),
			},
			"four": {
				name:                "four",
				lastWrittenRevision: revisions.NewForTimestamp(10),
			},
		}
		r := ds.SnapshotReader(testRevision)
		testListAllCaveats(t, r, expectedNs)
		testReadCaveatByName(t, r, expectedNs["four"])
		testLookupCaveatsWithNames(t, r,
			[]string{"four", "unknown"},
			map[string]expectCaveat{"four": expectedNs["four"]},
		)
	})

	t.Run("no caveats", func(t *testing.T) {
		testRevision := revisions.NewForTimestamp(7)
		expectedNs := map[string]expectCaveat{}
		r := ds.SnapshotReader(testRevision)
		testListAllCaveats(t, r, expectedNs)
		testReadCaveatByName(t, r, expectedNs["one"])
		testLookupCaveatsWithNames(t, r, []string{"one", "two"}, nil)
	})
}

// this is a very basic test to ensure yql works at all.
// full test suit is in github.com/authzed/spicedb/pkg/datastore/test.
func TestYDBReaderRelationships(t *testing.T) {
	type testRelationship struct {
		namespace        string
		objectID         string
		relation         string
		usersetNamespace string
		usersetObjectID  string
		usersetRelation  string
		caveatName       *string
		caveatContext    *[]byte
		createdAt        int64
		deletedAt        *int64
	}

	testRelationships := []testRelationship{
		0: {
			namespace:        "document",
			objectID:         "firstdoc",
			relation:         "reader",
			usersetNamespace: "user",
			usersetObjectID:  "bob",
			usersetRelation:  "...",
			caveatName:       proto.String("test"),
			caveatContext:    lo.ToPtr([]byte(`{"in":"bar"}`)),
			createdAt:        10,
		},
		1: {
			namespace:        "document",
			objectID:         "firstdoc",
			relation:         "writer",
			usersetNamespace: "user",
			usersetObjectID:  "alice",
			usersetRelation:  "...",
			createdAt:        10,
		},
		2: {
			namespace:        "document",
			objectID:         "firstdoc",
			relation:         "writer",
			usersetNamespace: "user",
			usersetObjectID:  "bob",
			usersetRelation:  "...",
			createdAt:        1,
			deletedAt:        proto.Int64(7),
		},
	}

	ds := ydbTestEngine.NewDatastore(t, func(engine, dsn string) datastore.Datastore {
		ds, err := NewYDBDatastore(context.Background(), dsn)
		require.NoError(t, err)

		yDS, err := newYDBDatastore(context.Background(), dsn)
		require.NoError(t, err)
		t.Cleanup(func() { yDS.Close() })

		err = yDS.driver.Table().Do(context.Background(), func(ctx context.Context, s table.Session) error {

			stmt, err := s.Prepare(
				ctx,
				common.AddTablePrefix(`
					DECLARE $namespace AS Utf8; 
					DECLARE $objectID AS Utf8; 
					DECLARE $relation AS Utf8; 
					DECLARE $usersetNamespace AS Utf8; 
					DECLARE $usersetObjectID AS Utf8; 
					DECLARE $usersetRelation AS Utf8; 
					DECLARE $caveatName AS Optional<Utf8>;
					DECLARE $caveatContext AS Optional<JsonDocument>;
					DECLARE $createdAt AS Int64;
					DECLARE $deletedAt AS Optional<Int64>;
					INSERT INTO
						relation_tuple(
							namespace, 
							object_id,
							relation,
							userset_namespace,
							userset_object_id,
							userset_relation,
							caveat_name,
							caveat_context,
							created_at_unix_nano,
							deleted_at_unix_nano
						)
					VALUES 
						(
							$namespace,
							$objectID,
							$relation,
							$usersetNamespace,
							$usersetObjectID,
							$usersetRelation,
							$caveatName,
							$caveatContext,
							$createdAt,
							$deletedAt
						);
				`, yDS.config.tablePathPrefix),
			)
			if err != nil {
				return err
			}

			for i := 0; i < len(testRelationships); i++ {
				_, _, err = stmt.Execute(
					ctx,
					table.DefaultTxControl(),
					table.NewQueryParameters(
						table.ValueParam("$namespace", types.UTF8Value(testRelationships[i].namespace)),
						table.ValueParam("$objectID", types.UTF8Value(testRelationships[i].objectID)),
						table.ValueParam("$relation", types.UTF8Value(testRelationships[i].relation)),
						table.ValueParam("$usersetNamespace", types.UTF8Value(testRelationships[i].usersetNamespace)),
						table.ValueParam("$usersetObjectID", types.UTF8Value(testRelationships[i].usersetObjectID)),
						table.ValueParam("$usersetRelation", types.UTF8Value(testRelationships[i].usersetRelation)),
						table.ValueParam("$caveatName", types.NullableUTF8Value(testRelationships[i].caveatName)),
						table.ValueParam("$caveatContext", types.NullableJSONDocumentValueFromBytes(testRelationships[i].caveatContext)),
						table.ValueParam("$createdAt", types.Int64Value(testRelationships[i].createdAt)),
						table.ValueParam("$deletedAt", types.NullableInt64Value(testRelationships[i].deletedAt)),
					),
				)
				if err != nil {
					return err
				}
			}

			return nil
		})
		require.NoError(t, err)

		return ds
	})
	t.Cleanup(func() { ds.Close() })

	matchRelationship := func(t *testing.T, expect testRelationship, actual *core.RelationTuple) {
		require.Equal(t, expect.namespace, actual.GetResourceAndRelation().GetNamespace())
		require.Equal(t, expect.objectID, actual.GetResourceAndRelation().GetObjectId())
		require.Equal(t, expect.relation, actual.GetResourceAndRelation().GetRelation())
		require.Equal(t, expect.usersetNamespace, actual.GetSubject().GetNamespace())
		require.Equal(t, expect.usersetObjectID, actual.GetSubject().GetObjectId())
		require.Equal(t, expect.usersetRelation, actual.GetSubject().GetRelation())
		require.Equal(t, lo.FromPtr(expect.caveatName), actual.GetCaveat().GetCaveatName())

		var actualCaveatContext []byte
		if actual.GetCaveat().GetContext() != nil {
			var err error
			actualCaveatContext, err = json.Marshal(actual.GetCaveat().GetContext().GetFields())
			require.NoError(t, err)
		}

		require.Equal(t, lo.FromPtr(expect.caveatContext), actualCaveatContext)

	}

	testQueryRelationships := func(
		t *testing.T,
		r datastore.Reader,
		f datastore.RelationshipsFilter,
		expect ...testRelationship,
	) {
		it, err := r.QueryRelationships(context.Background(), f)
		require.NoError(t, err)
		t.Cleanup(it.Close)

		var actual []*core.RelationTuple
		for v := it.Next(); v != nil; v = it.Next() {
			actual = append(actual, v)
		}
		require.Len(t, actual, len(expect))
		for i := range expect {
			matchRelationship(t, expect[i], actual[i])
		}
	}

	testReverseQueryRelationships := func(
		t *testing.T,
		r datastore.Reader,
		f datastore.SubjectsFilter,
		expect ...testRelationship,
	) {
		it, err := r.ReverseQueryRelationships(context.Background(), f)
		require.NoError(t, err)
		t.Cleanup(it.Close)

		var actual []*core.RelationTuple
		for v := it.Next(); v != nil; v = it.Next() {
			actual = append(actual, v)
		}
		require.Len(t, actual, len(expect))
		for i := range expect {
			matchRelationship(t, expect[i], actual[i])
		}
	}

	t.Run("removed relationships not garbage collected", func(t *testing.T) {
		testRevision := revisions.NewForTimestamp(6)
		r := ds.SnapshotReader(testRevision)
		testQueryRelationships(t, r, datastore.RelationshipsFilter{
			ResourceType:             "document",
			OptionalResourceRelation: "writer",
		}, testRelationships[2])
		testReverseQueryRelationships(t, r, datastore.SubjectsFilter{
			SubjectType: "user",
		}, testRelationships[2])
	})

	t.Run("latest relationships", func(t *testing.T) {
		testRevision := revisions.NewForTimestamp(10)
		r := ds.SnapshotReader(testRevision)
		testQueryRelationships(t, r, datastore.RelationshipsFilter{
			ResourceType:             "document",
			OptionalResourceRelation: "reader",
		}, testRelationships[0])
		testReverseQueryRelationships(t, r, datastore.SubjectsFilter{
			SubjectType:        "user",
			OptionalSubjectIds: []string{"bob"},
			RelationFilter: datastore.SubjectRelationFilter{
				IncludeEllipsisRelation: true,
			},
		}, testRelationships[0])
	})
}
