package ydb

import (
	"context"
	"fmt"

	yq "github.com/flymedllva/ydb-go-qb/yqb"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/internal/datastore/ydb/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type ydbReader struct {
}

func (y *ydbReader) ReadCaveatByName(ctx context.Context, name string) (caveat *core.CaveatDefinition, lastWritten datastore.Revision, err error) {
	// TODO implement me
	panic("implement me")
}

func (y *ydbReader) ListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	// TODO implement me
	panic("implement me")
}

func (y *ydbReader) LookupCaveatsWithNames(ctx context.Context, names []string) ([]datastore.RevisionedCaveat, error) {
	// TODO implement me
	panic("implement me")
}

func (y *ydbReader) QueryRelationships(ctx context.Context, filter datastore.RelationshipsFilter, options ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	// TODO implement me
	panic("implement me")
}

func (y *ydbReader) ReverseQueryRelationships(ctx context.Context, subjectsFilter datastore.SubjectsFilter, options ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	// TODO implement me
	panic("implement me")
}

func (y *ydbReader) ReadNamespaceByName(ctx context.Context, nsName string) (ns *core.NamespaceDefinition, lastWritten datastore.Revision, err error) {
	// TODO implement me
	panic("implement me")
}

func (y *ydbReader) ListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	// TODO implement me
	panic("implement me")
}

func (y *ydbReader) LookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	// TODO implement me
	panic("implement me")
}

var (
	readNamespaceBuilder = yq.Select("serialized_config", "created_at_unix_nano").From("namespace_config")
	livingObjectPred     = yq.Eq{"deleted_at_unix_nano": nil}
	livingObjectModifier = queryModifier(func(builder yq.SelectBuilder) yq.SelectBuilder {
		return builder.Where(livingObjectPred)
	})
)

func loadAllNamespaces(
	ctx context.Context,
	tablePathPrefix string,
	executor queryExecutor,
	modifier queryModifier,
) ([]datastore.RevisionedNamespace, error) {
	sql, args, err := modifier(readNamespaceBuilder).ToYdbSql()
	if err != nil {
		return nil, err
	}

	sql = common.AddTablePrefix(sql, tablePathPrefix)
	res, err := executor.Execute(ctx, sql, table.NewQueryParameters(args...))
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
