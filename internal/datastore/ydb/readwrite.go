package ydb

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jzelinskie/stringz"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	datastoreCommon "github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	ydbCommon "github.com/authzed/spicedb/internal/datastore/ydb/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

type ydbReadWriter struct {
	*ydbReader
	bulkLoadBatchSize int
	newRevision       revisions.TimestampRevision
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
		true,
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

// WriteRelationships takes a list of tuple mutations and applies them to the datastore.
func (rw *ydbReadWriter) WriteRelationships(ctx context.Context, mutations []*core.RelationTupleUpdate) error {
	insertBuilder := insertRelationsBuilder
	deleteBuilder := deleteRelationBuilder

	var (
		err             error
		insertionTuples []*core.RelationTuple
		touchTuples     []*core.RelationTuple

		deleteClauses sq.Or
	)

	// Parse the updates, building inserts for CREATE/TOUCH and deletes for DELETE.
	for _, mut := range mutations {
		tpl := mut.GetTuple()

		switch mut.GetOperation() {
		case core.RelationTupleUpdate_CREATE:
			insertionTuples = append(insertionTuples, tpl)
			insertBuilder, err = appendForInsertion(insertBuilder, tpl, rw.newRevision)
			if err != nil {
				return fmt.Errorf("failed to append tuple for insertion: %w", err)
			}

		case core.RelationTupleUpdate_TOUCH:
			touchTuples = append(touchTuples, tpl)

		case core.RelationTupleUpdate_DELETE:
			deleteClauses = append(deleteClauses, exactRelationshipClause(tpl))

		default:
			return spiceerrors.MustBugf("unknown tuple mutation: %v", mut)
		}
	}

	// Perform SELECT queries first as a part of uniqueness check.
	if len(insertionTuples) > 0 {
		dups, err := rw.selectTuples(ctx, insertionTuples)
		if err != nil {
			return fmt.Errorf("failed to ensure CREATE tuples uniqueness: %w", err)
		}
		if len(dups) > 0 {
			return datastoreCommon.NewCreateRelationshipExistsError(dups[0])
		}
	}
	if len(touchTuples) > 0 {
		dups, err := rw.selectTuples(ctx, touchTuples)
		if err != nil {
			return fmt.Errorf("failed to ensure TOUCH tuples uniqueness: %w", err)
		}

		duplicatePKToRel := make(map[string]*core.RelationTuple, len(dups))
		for i := range dups {
			duplicatePKToRel[tuple.StringWithoutCaveat(dups[i])] = dups[i]
		}

		for i := 0; i < len(touchTuples); {
			pk := tuple.StringWithoutCaveat(touchTuples[i])
			dup, ok := duplicatePKToRel[pk]
			if !ok {
				insertBuilder, err = appendForInsertion(insertBuilder, touchTuples[i], rw.newRevision)
				if err != nil {
					return fmt.Errorf("failed to append tuple for insertion: %w", err)
				}
				i++
				continue
			}

			if tuple.Equal(touchTuples[i], dup) {
				touchTuples[i], touchTuples[len(touchTuples)-1] = touchTuples[len(touchTuples)-1], touchTuples[i]
				touchTuples = touchTuples[:len(touchTuples)-1]
				continue
			}

			deleteClauses = append(deleteClauses, exactRelationshipClause(dup))
			insertBuilder, err = appendForInsertion(insertBuilder, touchTuples[i], rw.newRevision)
			if err != nil {
				return fmt.Errorf("failed to append tuple for insertion: %w", err)
			}
			i++
		}
	}

	// Run DELETE updates, if any.
	if len(deleteClauses) > 0 {
		if err := executeDeleteQuery(
			ctx,
			rw.tablePathPrefix,
			rw.executor,
			deleteBuilder,
			rw.newRevision,
			deleteClauses,
		); err != nil {
			return fmt.Errorf("failed to delete tuples: %w", err)
		}
	}

	// Run CREATE and TOUCH insertions, if any.
	if len(insertionTuples) > 0 || len(touchTuples) > 0 {
		if err := executeQuery(ctx, rw.tablePathPrefix, rw.executor, insertBuilder); err != nil {
			return fmt.Errorf("failed to insert tuples: %w", err)
		}
	}

	return nil
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
		false,
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

// BulkLoad takes a relationship source iterator, and writes all the
// relationships to the backing datastore in an optimized fashion. This
// method can and will omit checks and otherwise cut corners in the
// interest of performance, and should not be relied upon for OLTP-style
// workloads.
// For YDB this is not an effective way insert relationships.
// Recommended insert bulk size is < 100k per transaction. Under the hood this method
// works just like WriteRelationships with CREATE operation splitting input into a batches.
func (rw *ydbReadWriter) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	var (
		insertionTuples []*core.RelationTuple
		tpl             *core.RelationTuple
		err             error
		totalCount      int
	)

	tpl, err = iter.Next(ctx)
	for tpl != nil && err == nil {
		insertionTuples = insertionTuples[:0]
		insertBuilder := insertRelationsBuilder

		for ; tpl != nil && err == nil && len(insertionTuples) < rw.bulkLoadBatchSize; tpl, err = iter.Next(ctx) {
			// need to copy, see datastore.BulkWriteRelationshipSource docs
			insertionTuples = append(insertionTuples, tpl.CloneVT())
			insertBuilder, err = appendForInsertion(insertBuilder, tpl, rw.newRevision)
			if err != nil {
				return 0, fmt.Errorf("failed to append tuple for insertion: %w", err)
			}
		}
		if err != nil {
			return 0, fmt.Errorf("failed to read source: %w", err)
		}

		if len(insertionTuples) > 0 {
			dups, err := rw.selectTuples(ctx, insertionTuples)
			if err != nil {
				return 0, fmt.Errorf("failed to ensure CREATE tuples uniqueness: %w", err)
			}
			if len(dups) > 0 {
				return 0, datastoreCommon.NewCreateRelationshipExistsError(dups[0])
			}

			if err := executeQuery(ctx, rw.tablePathPrefix, rw.executor, insertBuilder); err != nil {
				return 0, fmt.Errorf("failed to insert tuples: %w", err)
			}
		}

		totalCount += len(insertionTuples)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to read source: %w", err)
	}

	return uint64(totalCount), nil
}

func (rw *ydbReadWriter) selectTuples(
	ctx context.Context,
	in []*core.RelationTuple,
) ([]*core.RelationTuple, error) {
	ctx, span := tracer.Start(ctx, "selectTuples", trace.WithAttributes(attribute.Int("count", len(in))))
	defer span.End()

	if len(in) == 0 {
		return nil, nil
	}

	var pred sq.Or
	for _, r := range in {
		pred = append(pred, exactRelationshipClause(r))
	}

	sql, args, err := rw.modifier(readRelationBuilder).View(ixUqRelationLiving).Where(pred).ToYQL()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	return queryTuples(ctx, rw.tablePathPrefix, sql, table.NewQueryParameters(args...), span, rw.executor)
}

type coreDefinition interface {
	MarshalVT() ([]byte, error)
	GetName() string
	proto.Message
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
	useVT bool,
) error {
	ctx, span := tracer.Start(ctx, "writeDefinitions", trace.WithAttributes(attribute.Int("count", len(defs))))
	defer span.End()

	if len(defs) == 0 {
		return nil
	}

	// hack b/c namespace and caveat use different marshal method
	var marshaler func(def coreDefinition) ([]byte, error)
	if useVT {
		marshaler = func(def coreDefinition) ([]byte, error) {
			return def.MarshalVT()
		}
	} else {
		marshaler = func(def coreDefinition) ([]byte, error) {
			return proto.Marshal(def)
		}
	}

	names := make([]string, 0, len(defs))
	for _, def := range defs {
		definitionBytes, err := marshaler(def)
		if err != nil {
			return fmt.Errorf("failed to marshal definition: %w", err)
		}

		b = b.Values(def.GetName(), definitionBytes, writeRev.TimestampNanoSec())
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

	sql, args, err := b.ToYQL()
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

func appendForInsertion(
	b sq.InsertBuilder,
	t *core.RelationTuple,
	rev revisions.TimestampRevision,
) (sq.InsertBuilder, error) {
	var caveatName *string
	var caveatContext *[]byte
	if t.GetCaveat() != nil {
		caveatName = &t.GetCaveat().CaveatName
		cc, err := t.GetCaveat().GetContext().MarshalJSON()
		if err != nil {
			return b, err
		}
		caveatContext = &cc
	}

	valuesToWrite := []interface{}{
		t.GetResourceAndRelation().GetNamespace(),
		t.GetResourceAndRelation().GetObjectId(),
		t.GetResourceAndRelation().GetRelation(),
		t.GetSubject().GetNamespace(),
		t.GetSubject().GetObjectId(),
		t.GetSubject().GetRelation(),
		caveatName,
		types.NullableJSONDocumentValueFromBytes(caveatContext),
		rev.TimestampNanoSec(),
	}

	return b.Values(valuesToWrite...), nil
}

func exactRelationshipClause(r *core.RelationTuple) sq.Eq {
	return sq.Eq{
		colNamespace:        r.GetResourceAndRelation().GetNamespace(),
		colObjectID:         r.GetResourceAndRelation().GetObjectId(),
		colRelation:         r.GetResourceAndRelation().GetRelation(),
		colUsersetNamespace: r.GetSubject().GetNamespace(),
		colUsersetObjectID:  r.GetSubject().GetObjectId(),
		colUsersetRelation:  r.GetSubject().GetRelation(),
	}
}
