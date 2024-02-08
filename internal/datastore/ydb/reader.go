package ydb

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func loadAllNamespaces(
	ctx context.Context,
	tx table.TransactionActor,
	nsQuery string,
) ([]datastore.RevisionedNamespace, error) {
	res, err := tx.Execute(ctx, nsQuery, nil)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var nsDefs []datastore.RevisionedNamespace
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var config []byte
			var createdAtUnixNano int64
			if err := res.Scan(&config, &createdAtUnixNano); err != nil {
				return nil, err
			}

			var loaded core.NamespaceDefinition
			if err := loaded.UnmarshalVT(config); err != nil {
				return nil, fmt.Errorf("failed to read namespace config: %w", err)
			}

			revision := revisions.NewForTimestamp(createdAtUnixNano)
			nsDefs = append(nsDefs, datastore.RevisionedNamespace{Definition: &loaded, LastWrittenRevision: revision})
		}
	}

	if err := res.Err(); err != nil {
		return nil, err
	}

	return nsDefs, nil
}
