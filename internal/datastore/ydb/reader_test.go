package ydb

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/internal/datastore/ydb/common"
	testserverDatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestYDBReaderNamespaces(t *testing.T) {
	engine := testserverDatastore.RunYDBForTesting(t, "")

	ds := engine.NewDatastore(t, func(engine, dsn string) datastore.Datastore {
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

	matchNamspace := func(t *testing.T, expect expectNs, actual datastore.RevisionedNamespace) {
		require.Equal(t, expect.lastWrittenRevision, actual.LastWrittenRevision)
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
			matchNamspace(t, expectNs, ns)
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
			matchNamspace(t, expectNs, ns)
		}
	}

	testReadNamespaceByName := func(t *testing.T, r datastore.Reader, expect expectNs) {
		ns, lastWritten, err := r.ReadNamespaceByName(context.Background(), expect.name)
		if expect.name == "" {
			require.ErrorAs(t, err, new(datastore.ErrNotFound))
			return
		}
		require.NoError(t, err)
		matchNamspace(t, expect, datastore.RevisionedNamespace{
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
