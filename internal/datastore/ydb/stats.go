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
	)

	var stats datastore.Stats

	err := y.driver.Table().DoTx(
		ctx,
		func(ctx context.Context, tx table.TransactionActor) error {
			if err := queryRowTx(
				ctx,
				tx,
				common.RewriteQuery(queryUniqueID, y.config.tablePathPrefix),
				nil,
				&stats.UniqueID,
			); err != nil {
				return err
			}

			if err := queryRowTx(
				ctx,
				tx,
				queryEstimatedRelationshipCount, // no need to rewrite this query
				table.NewQueryParameters(
					table.ValueParam("$table_path_prefix", types.TextValue(y.config.tablePathPrefix)),
				),
				&stats.EstimatedRelationshipCount,
			); err != nil {
				return err
			}

			return nil
		},
		table.WithTxSettings(table.TxSettings(table.WithSnapshotReadOnly())))
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("failed to query datastore stats: %w", err)
	}

	return stats, nil
}
