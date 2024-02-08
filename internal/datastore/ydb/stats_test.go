package ydb

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"github.com/authzed/spicedb/internal/datastore/ydb/common"
	testserverDatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
)

func TestYDBDatastoreStatistics(t *testing.T) {
	engine := testserverDatastore.RunYDBForTesting(t, "")

	ds := engine.NewDatastore(t, func(engine, dsn string) datastore.Datastore {
		ds, err := NewYDBDatastore(context.Background(), dsn)
		require.NoError(t, err)

		yDS, err := newYDBDatastore(context.Background(), dsn)
		require.NoError(t, err)
		t.Cleanup(func() { yDS.Close() })

		err = yDS.driver.Table().Do(context.Background(), func(ctx context.Context, s table.Session) error {
			// taken from spicedb playground example
			testNS := []struct {
				name         string
				configBase64 string
			}{
				{
					name:         "user",
					configBase64: "CgR1c2Vy",
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
				},
			}

			stmt, err := s.Prepare(
				ctx,
				common.AddTablePrefix(`
					DECLARE $name AS Utf8;
					DECLARE $config AS String;
					INSERT INTO
						namespace_config(namespace, serialized_config, created_at_unix_nano, deleted_at_unix_nano)
					VALUES 
						($name, $config, 1, NULL);
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
					),
				)
				if err != nil {
					return err
				}
			}

			testRels := []struct {
				namespace        string
				objectID         string
				relation         string
				usersetNamespace string
				usersetObjectID  string
			}{
				{
					namespace:        "document",
					objectID:         "firstdoc",
					relation:         "writer",
					usersetNamespace: "user",
					usersetObjectID:  "tom",
				},
				{
					namespace:        "document",
					objectID:         "firstdoc",
					relation:         "reader",
					usersetNamespace: "user",
					usersetObjectID:  "fred",
				},
				{
					namespace:        "document",
					objectID:         "seconddoc",
					relation:         "reader",
					usersetNamespace: "user",
					usersetObjectID:  "tom",
				},
			}

			stmt, err = s.Prepare(
				ctx,
				common.AddTablePrefix(`
					DECLARE $namespace AS Utf8;
					DECLARE $objectID AS Utf8;
					DECLARE $relation AS Utf8;
					DECLARE $usersetNamespace AS Utf8;
					DECLARE $usersetObjectID AS Utf8;
					INSERT INTO
						relation_tuple(
							namespace,
							object_id,
							relation,
							userset_namespace,
							userset_object_id,
							userset_relation,
							created_at_unix_nano,
							deleted_at_unix_nano
						)
					VALUES 
						($namespace, $objectID, $relation, $usersetNamespace, $usersetObjectID, '...', 1, NULL);
				`, yDS.config.tablePathPrefix),
			)
			if err != nil {
				return err
			}

			for i := 0; i < len(testRels); i++ {
				_, _, err = stmt.Execute(
					ctx,
					table.DefaultTxControl(),
					table.NewQueryParameters(
						table.ValueParam("$namespace", types.UTF8Value(testRels[i].namespace)),
						table.ValueParam("$objectID", types.UTF8Value(testRels[i].objectID)),
						table.ValueParam("$relation", types.UTF8Value(testRels[i].relation)),
						table.ValueParam("$usersetNamespace", types.UTF8Value(testRels[i].usersetNamespace)),
						table.ValueParam("$usersetObjectID", types.UTF8Value(testRels[i].usersetObjectID)),
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

	stats, err := ds.Statistics(context.Background())
	require.NoError(t, err)

	require.NotEmpty(t, stats.UniqueID)
	t.Logf("unique_id: %s", stats.UniqueID)
	// there's no way to force YDB to analyze table
	require.True(t, stats.EstimatedRelationshipCount == 0 || stats.EstimatedRelationshipCount == 3)
	require.ElementsMatch(t, stats.ObjectTypeStatistics, []datastore.ObjectTypeStat{
		{
			NumRelations:   0,
			NumPermissions: 0,
		},
		{
			NumRelations:   2,
			NumPermissions: 2,
		},
	})
}
