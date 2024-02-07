package ydb

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"github.com/authzed/spicedb/internal/datastore/ydb/common"
	"github.com/authzed/spicedb/pkg/datastore"
)

func (y *ydbDatastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	const (
		queryUniqueID = "SELECT unique_id FROM metadata"

		queryEstimatedRelationshipCount = `
DECLARE $table_path_prefix AS Utf8;
$stats_table_name = ".sys/partition_stats";
SELECT 
	Unwrap(SUM(RowCount))
FROM 
	$stats_table_name
WHERE 
	Path = $table_path_prefix || '/relation_tuple';
`
		queryLiveNamespaces = `
SELECT 
    serialized_config,
    created_at_unix_nano 
FROM 
    namespace_config
WHERE 
    deleted_at_unix_nano IS NULL
`
	)

	var (
		nsDefs   []datastore.RevisionedNamespace
		uniqueID string
		relCount uint64
	)

	err := y.driver.Table().DoTx(
		ctx,
		func(ctx context.Context, tx table.TransactionActor) error {
			err := queryRowTx(
				ctx,
				tx,
				common.RewriteQuery(queryUniqueID, y.config.tablePathPrefix),
				nil,
				&uniqueID,
			)
			if err != nil {
				return err
			}

			err = queryRowTx(
				ctx,
				tx,
				queryEstimatedRelationshipCount, // no need to rewrite this query
				table.NewQueryParameters(
					table.ValueParam("$table_path_prefix", types.TextValue(y.config.tablePathPrefix)),
				),
				&relCount,
			)
			if err != nil {
				return err
			}

			nsDefs, err = loadAllNamespaces(
				ctx,
				tx,
				common.RewriteQuery(queryLiveNamespaces, y.config.tablePathPrefix),
			)
			if err != nil {
				return err
			}

			return nil
		},
		table.WithTxSettings(table.TxSettings(table.WithSnapshotReadOnly())))
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("failed to query datastore stats: %w", err)
	}

	return datastore.Stats{
		UniqueID:                   uniqueID,
		EstimatedRelationshipCount: relCount,
		ObjectTypeStatistics:       datastore.ComputeObjectTypeStats(nsDefs),
	}, nil
}
