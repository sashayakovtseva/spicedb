package ydb

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	ydbCommon "github.com/authzed/spicedb/internal/datastore/ydb/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type ydbReadWriter struct {
	*ydbReader
	newRevision revisions.TimestampRevision
}

// WriteCaveats stores the provided caveats.
// Currently living caveats with the same names are silently marked as deleted (table range scan operation).
func (rw *ydbReadWriter) WriteCaveats(ctx context.Context, caveats []*core.CaveatDefinition) error {
	if len(caveats) == 0 {
		return nil
	}

	b := insertCaveatBuilder
	caveatNames := make([]string, 0, len(caveats))
	for _, caveat := range caveats {
		definitionBytes, err := caveat.MarshalVT()
		if err != nil {
			return fmt.Errorf("failed to marshal caveat definition: %w", err)
		}

		valuesToWrite := []any{caveat.Name, definitionBytes, rw.newRevision.TimestampNanoSec()}
		b = b.Values(valuesToWrite...)
		caveatNames = append(caveatNames, caveat.Name)
	}

	// note: there's no need in select ensure with this removal.
	if err := executeDeleteQuery(
		ctx,
		rw.tablePathPrefix,
		rw.executor,
		deleteCaveatBuilder,
		rw.newRevision,
		sq.Eq{colName: caveatNames},
	); err != nil {
		return fmt.Errorf("failed to delete existing caveats: %w", err)
	}

	sql, args, err := b.ToYdbSql()
	if err != nil {
		return fmt.Errorf("failed to build insert caveats query: %w", err)
	}

	sql = ydbCommon.AddTablePrefix(sql, rw.tablePathPrefix)
	res, err := rw.executor.Execute(ctx, sql, table.NewQueryParameters(args...))
	if err != nil {
		return fmt.Errorf("failed to insert caveats: %w", err)
	}
	defer res.Close()

	return nil
}

// DeleteCaveats deletes the provided caveats by name.
// This is a table range scan operation.
func (rw *ydbReadWriter) DeleteCaveats(ctx context.Context, names []string) error {
	return executeDeleteQuery(
		ctx,
		rw.tablePathPrefix,
		rw.executor,
		deleteCaveatBuilder,
		rw.newRevision,
		sq.Eq{colName: names},
	)
}

func (rw *ydbReadWriter) WriteRelationships(ctx context.Context, mutations []*core.RelationTupleUpdate) error {
	// TODO implement me
	panic("implement me")
}

func (rw *ydbReadWriter) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter) error {
	// TODO implement me
	panic("implement me")
}

func (rw *ydbReadWriter) WriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	// TODO implement me
	panic("implement me")
}

// DeleteNamespaces deletes namespaces including associated relationships.
func (rw *ydbReadWriter) DeleteNamespaces(ctx context.Context, names ...string) error {
	if err := executeDeleteQuery(
		ctx,
		rw.tablePathPrefix,
		rw.executor,
		deleteNamespaceBuilder,
		rw.newRevision,
		sq.Eq{colNamespace: names},
	); err != nil {
		return fmt.Errorf("failed to delete namespaces: %w", err)
	}

	if err := executeDeleteQuery(
		ctx,
		rw.tablePathPrefix,
		rw.executor,
		deleteRelationBuilder,
		rw.newRevision,
		sq.Eq{colNamespace: names},
	); err != nil {
		return fmt.Errorf("failed to delete namespace relations: %w", err)
	}
	return nil
}

func (rw *ydbReadWriter) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	// TODO implement me
	panic("implement me")
}
