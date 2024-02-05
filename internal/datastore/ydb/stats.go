package ydb

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"github.com/authzed/spicedb/pkg/datastore"
)

func (y *ydbDatastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	const queryUniqueID = "SELECT CAST(unique_id AS Utf8) FROM metadata"

	var stats datastore.Stats

	err := y.driver.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		res, err := tx.Execute(ctx, queryUniqueID, nil)
		if err != nil {
			return err
		}
		if err := res.NextResultSetErr(ctx); err != nil {
			return err
		}
		if !res.NextRow() {
			return fmt.Errorf("no unique id rows")
		}
		if err := res.Scan(&stats.UniqueID); err != nil {
			return err
		}
		if err := res.Err(); err != nil {
			return err
		}
		if err := res.Close(); err != nil {
			return err
		}

		return nil
	}, table.WithTxSettings(table.TxSettings(table.WithSnapshotReadOnly())))
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("failed to query datastore stats: %w", err)
	}
	return stats, nil
}
