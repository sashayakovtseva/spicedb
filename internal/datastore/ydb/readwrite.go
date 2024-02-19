package ydb

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jzelinskie/stringz"
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
	return writeDefinitions[*core.CaveatDefinition](
		ctx,
		rw.tablePathPrefix,
		rw.executor,
		rw.newRevision,
		insertCaveatBuilder,
		deleteCaveatBuilder,
		colName,
		caveats,
	)
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

// DeleteRelationships deletes all Relationships that match the provided filter.
func (rw *ydbReadWriter) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter) error {
	pred := sq.Eq{
		colNamespace: filter.GetResourceType(),
	}

	if filter.GetOptionalResourceId() != "" {
		pred[colObjectID] = filter.GetOptionalResourceId()
	}
	if filter.GetOptionalRelation() != "" {
		pred[colRelation] = filter.GetOptionalRelation()
	}

	if subjectFilter := filter.GetOptionalSubjectFilter(); subjectFilter != nil {
		pred[colUsersetNamespace] = subjectFilter.GetSubjectType()
		if subjectFilter.GetOptionalSubjectId() != "" {
			pred[colUsersetObjectID] = subjectFilter.GetOptionalSubjectId()
		}
		if relationFilter := subjectFilter.GetOptionalRelation(); relationFilter != nil {
			pred[colUsersetRelation] = stringz.DefaultEmpty(relationFilter.GetRelation(), datastore.Ellipsis)
		}
	}

	if err := executeDeleteQuery(
		ctx,
		rw.tablePathPrefix,
		rw.executor,
		deleteRelationBuilder,
		rw.newRevision,
		pred,
	); err != nil {
		return fmt.Errorf("failed to delete relations: %w", err)
	}

	return nil
}

// WriteNamespaces takes proto namespace definitions and persists them.
func (rw *ydbReadWriter) WriteNamespaces(ctx context.Context, namespaces ...*core.NamespaceDefinition) error {
	return writeDefinitions[*core.NamespaceDefinition](
		ctx,
		rw.tablePathPrefix,
		rw.executor,
		rw.newRevision,
		insertNamespaceBuilder,
		deleteNamespaceBuilder,
		colNamespace,
		namespaces,
	)
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

type coreDefinition interface {
	MarshalVT() ([]byte, error)
	GetName() string
}

// writeDefinitions is a generic func to upsert caveats or namespaces.
func writeDefinitions[T coreDefinition](
	ctx context.Context,
	tablePathPrefix string,
	executor queryExecutor,
	writeRev revisions.TimestampRevision,
	b sq.InsertBuilder,
	d sq.UpdateBuilder,
	colName string,
	defs []T,
) error {
	if len(defs) == 0 {
		return nil
	}

	names := make([]string, 0, len(defs))
	for _, def := range defs {
		definitionBytes, err := def.MarshalVT()
		if err != nil {
			return fmt.Errorf("failed to marshal definition: %w", err)
		}

		valuesToWrite := []any{def.GetName(), definitionBytes, writeRev.TimestampNanoSec()}
		b = b.Values(valuesToWrite...)
		names = append(names, def.GetName())
	}

	// note: there's no need in select ensure with this removal.
	if err := executeDeleteQuery(
		ctx,
		tablePathPrefix,
		executor,
		d,
		writeRev,
		sq.Eq{colName: names},
	); err != nil {
		return fmt.Errorf("failed to delete existing definitions: %w", err)
	}

	sql, args, err := b.ToYdbSql()
	if err != nil {
		return fmt.Errorf("failed to build insert definitions query: %w", err)
	}

	sql = ydbCommon.AddTablePrefix(sql, tablePathPrefix)
	res, err := executor.Execute(ctx, sql, table.NewQueryParameters(args...))
	if err != nil {
		return fmt.Errorf("failed to insert definitions: %w", err)
	}
	defer res.Close()

	return nil
}
