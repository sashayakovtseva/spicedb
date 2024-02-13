package ydb

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	ydbCommon "github.com/authzed/spicedb/internal/datastore/ydb/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type ydbReader struct {
	tablePathPrefix string
	executor        queryExecutor
	modifier        queryModifier
	// commonQueryExecutor is used in QueryRelationships.
	// Basically it is a wrapper around queryExecutor in order to reuse common codebase.
	commonQueryExecutor common.QueryExecutor
}

func newYDBReader(tablePathPrefix string, executor queryExecutor, modifier queryModifier) *ydbReader {
	return &ydbReader{
		tablePathPrefix: tablePathPrefix,
		executor:        executor,
		modifier:        modifier,
		commonQueryExecutor: common.QueryExecutor{
			Executor: newYDBCommonQueryExecutor(tablePathPrefix, executor),
			ToSQL:    toYQLWrapper,
		},
	}
}

func (r *ydbReader) ReadCaveatByName(
	ctx context.Context,
	name string,
) (*core.CaveatDefinition, datastore.Revision, error) {
	loaded, version, err := r.loadCaveat(ctx, name)
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf("filed to read caveat: %w", err)
	}
	return loaded, version, nil
}

func (r *ydbReader) ListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	caveatsWithRevisions, err := loadAllCaveats(ctx, r.tablePathPrefix, r.executor, r.modifier)
	if err != nil {
		return nil, fmt.Errorf("failed to list caveats: %w", err)
	}

	return caveatsWithRevisions, err
}

func (r *ydbReader) LookupCaveatsWithNames(ctx context.Context, names []string) ([]datastore.RevisionedCaveat, error) {
	if len(names) == 0 {
		return nil, nil
	}

	var clause sq.Or
	for _, nsName := range names {
		clause = append(clause, sq.Eq{colName: nsName})
	}

	caveatsWithRevisions, err := loadAllCaveats(
		ctx,
		r.tablePathPrefix,
		r.executor,
		func(builder sq.SelectBuilder) sq.SelectBuilder {
			return r.modifier(builder).Where(clause)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list caveats with names: %w", err)
	}

	return caveatsWithRevisions, err
}

func (r *ydbReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	qBuilder, err := common.NewSchemaQueryFilterer(
		relationTupleSchema,
		r.modifier(readRelationBuilder),
	).FilterWithRelationshipsFilter(filter)
	if err != nil {
		return nil, err
	}

	return r.commonQueryExecutor.ExecuteQuery(ctx, qBuilder, opts...)
}

func (r *ydbReader) ReverseQueryRelationships(ctx context.Context, subjectsFilter datastore.SubjectsFilter, options ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	// TODO implement me
	panic("implement me")
}

func (r *ydbReader) ReadNamespaceByName(
	ctx context.Context,
	nsName string,
) (*core.NamespaceDefinition, datastore.Revision, error) {
	loaded, version, err := r.loadNamespace(ctx, nsName)
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf("filed to read namespace: %w", err)
	}
	return loaded, version, nil
}

func (r *ydbReader) ListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	nsDefsWithRevisions, err := loadAllNamespaces(ctx, r.tablePathPrefix, r.executor, r.modifier)
	if err != nil {
		return nil, fmt.Errorf("failed to list namespaces: %w", err)
	}

	return nsDefsWithRevisions, err
}

func (r *ydbReader) LookupNamespacesWithNames(
	ctx context.Context,
	nsNames []string,
) ([]datastore.RevisionedNamespace, error) {
	if len(nsNames) == 0 {
		return nil, nil
	}

	var clause sq.Or
	for _, nsName := range nsNames {
		clause = append(clause, sq.Eq{colNamespace: nsName})
	}

	nsDefsWithRevisions, err := loadAllNamespaces(
		ctx,
		r.tablePathPrefix,
		r.executor,
		func(builder sq.SelectBuilder) sq.SelectBuilder {
			return r.modifier(builder).Where(clause)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list namespaces with names: %w", err)
	}

	return nsDefsWithRevisions, err
}

func (r *ydbReader) loadNamespace(
	ctx context.Context,
	namespace string,
) (*core.NamespaceDefinition, revisions.TimestampRevision, error) {
	ctx, span := tracer.Start(ctx, "loadNamespace")
	defer span.End()

	defs, err := loadAllNamespaces(
		ctx,
		r.tablePathPrefix,
		r.executor,
		func(builder sq.SelectBuilder) sq.SelectBuilder {
			return r.modifier(builder).Where(sq.Eq{colNamespace: namespace})
		},
	)
	if err != nil {
		return nil, revisions.TimestampRevision(0), err
	}

	if len(defs) < 1 {
		return nil, revisions.TimestampRevision(0), datastore.NewNamespaceNotFoundErr(namespace)
	}

	return defs[0].Definition, defs[0].LastWrittenRevision.(revisions.TimestampRevision), nil
}

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

	sql = ydbCommon.AddTablePrefix(sql, tablePathPrefix)
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

func (r *ydbReader) loadCaveat(
	ctx context.Context,
	name string,
) (*core.CaveatDefinition, revisions.TimestampRevision, error) {
	ctx, span := tracer.Start(ctx, "loadCaveat")
	defer span.End()

	defs, err := loadAllCaveats(
		ctx,
		r.tablePathPrefix,
		r.executor,
		func(builder sq.SelectBuilder) sq.SelectBuilder {
			return r.modifier(builder).Where(sq.Eq{colName: name})
		},
	)
	if err != nil {
		return nil, revisions.TimestampRevision(0), err
	}

	if len(defs) < 1 {
		return nil, revisions.TimestampRevision(0), datastore.NewCaveatNameNotFoundErr(name)
	}

	return defs[0].Definition, defs[0].LastWrittenRevision.(revisions.TimestampRevision), nil
}

func loadAllCaveats(
	ctx context.Context,
	tablePathPrefix string,
	executor queryExecutor,
	modifier queryModifier,
) ([]datastore.RevisionedCaveat, error) {
	sql, args, err := modifier(readCaveatBuilder).ToYdbSql()
	if err != nil {
		return nil, err
	}

	sql = ydbCommon.AddTablePrefix(sql, tablePathPrefix)
	res, err := executor.Execute(ctx, sql, table.NewQueryParameters(args...))
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var caveatDefs []datastore.RevisionedCaveat
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var definition []byte
			var createdAtUnixNano int64
			if err := res.Scan(&definition, &createdAtUnixNano); err != nil {
				return nil, err
			}

			var loaded core.CaveatDefinition
			if err := loaded.UnmarshalVT(definition); err != nil {
				return nil, fmt.Errorf("failed to read caveat definition: %w", err)
			}

			revision := revisions.NewForTimestamp(createdAtUnixNano)
			caveatDefs = append(caveatDefs, datastore.RevisionedCaveat{
				Definition:          &loaded,
				LastWrittenRevision: revision,
			})
		}
	}

	if err := res.Err(); err != nil {
		return nil, err
	}

	return caveatDefs, nil
}
