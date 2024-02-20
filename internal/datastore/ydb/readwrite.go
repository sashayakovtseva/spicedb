package ydb

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jzelinskie/stringz"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.opentelemetry.io/otel/trace"

	datastoreCommon "github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	ydbCommon "github.com/authzed/spicedb/internal/datastore/ydb/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
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
	// touchMutationsByNonCaveat := make(map[string]*core.RelationTupleUpdate, len(mutations))

	insertBuilder := insertRelationsBuilder

	// touchInserts := insertRelationsBuilder
	// deleteClauses := sq.Or{}
	//

	var insertionTuples []*core.RelationTuple

	// Parse the updates, building inserts for CREATE/TOUCH and deletes for DELETE.
	for _, mut := range mutations {
		tpl := mut.GetTuple()

		switch mut.GetOperation() {
		case core.RelationTupleUpdate_CREATE:
			insertBuilder = appendForInsertion(insertBuilder, tpl, rw.newRevision)
			insertionTuples = append(insertionTuples, tpl)

		// case core.RelationTupleUpdate_TOUCH:
		// 	touchMutationsByNonCaveat[tuple.StringWithoutCaveat(tpl)] = mut
		// 	touchInserts = appendForInsertion(touchInserts, tpl, rw.newRevision)
		//
		// case core.RelationTupleUpdate_DELETE:
		// 	deleteClauses = append(deleteClauses, sq.Eq{
		// 		colNamespace:        tpl.GetResourceAndRelation().GetNamespace(),
		// 		colObjectID:         tpl.GetResourceAndRelation().GetObjectId(),
		// 		colRelation:         tpl.GetResourceAndRelation().GetRelation(),
		// 		colUsersetNamespace: tpl.GetSubject().GetNamespace(),
		// 		colUsersetObjectID:  tpl.GetSubject().GetObjectId(),
		// 		colUsersetRelation:  tpl.GetSubject().GetRelation(),
		// 	})

		default:
			return spiceerrors.MustBugf("unknown tuple mutation: %v", mut)
		}
	}

	// Perform SELECT queries first as a part of uniqueness check.
	if len(insertionTuples) > 0 {
		dups, err := rw.selectTuples(ctx, insertionTuples)
		if err != nil {
			return fmt.Errorf("failed to ensure tuples uniqueness: %w", err)
		}
		if len(dups) > 0 {
			return datastoreCommon.NewCreateRelationshipExistsError(dups[0])
		}
	}

	// Run CREATE insertions, if any.
	if len(insertionTuples) > 0 {
		if err := executeQuery(ctx, rw.tablePathPrefix, rw.executor, insertBuilder); err != nil {
			return fmt.Errorf("failed to insert tuples: %w", err)
		}
	}

	// // For each of the TOUCH operations, invoke the INSERTs, but with `ON CONFLICT DO NOTHING` to ensure
	// // that the operations over existing relationships no-op.
	// if len(touchMutationsByNonCaveat) > 0 {
	// 	touchInserts = touchInserts.Suffix(fmt.Sprintf("ON CONFLICT DO NOTHING RETURNING %s, %s, %s, %s, %s, %s",
	// 		colNamespace,
	// 		colObjectID,
	// 		colRelation,
	// 		colUsersetNamespace,
	// 		colUsersetObjectID,
	// 		colUsersetRelation,
	// 	))
	//
	// 	sql, args, err := touchInserts.ToSql()
	// 	if err != nil {
	// 		return fmt.Errorf(errUnableToWriteRelationships, err)
	// 	}
	//
	// 	rows, err := rwt.tx.Query(ctx, sql, args...)
	// 	if err != nil {
	// 		return fmt.Errorf(errUnableToWriteRelationships, err)
	// 	}
	// 	defer rows.Close()
	//
	// 	// Remove from the TOUCH map of operations each row that was successfully inserted.
	// 	// This will cover any TOUCH that created an entirely new relationship, acting like
	// 	// a CREATE.
	// 	tpl := &core.RelationTuple{
	// 		ResourceAndRelation: &core.ObjectAndRelation{},
	// 		Subject:             &core.ObjectAndRelation{},
	// 	}
	//
	// 	for rows.Next() {
	// 		err := rows.Scan(
	// 			&tpl.ResourceAndRelation.Namespace,
	// 			&tpl.ResourceAndRelation.ObjectId,
	// 			&tpl.ResourceAndRelation.Relation,
	// 			&tpl.Subject.Namespace,
	// 			&tpl.Subject.ObjectId,
	// 			&tpl.Subject.Relation,
	// 		)
	// 		if err != nil {
	// 			return fmt.Errorf(errUnableToWriteRelationships, err)
	// 		}
	//
	// 		tplString := tuple.StringWithoutCaveat(tpl)
	// 		_, ok := touchMutationsByNonCaveat[tplString]
	// 		if !ok {
	// 			return spiceerrors.MustBugf("missing expected completed TOUCH mutation")
	// 		}
	//
	// 		delete(touchMutationsByNonCaveat, tplString)
	// 	}
	// 	rows.Close()
	//
	// 	// For each remaining TOUCH mutation, add a DELETE operation for the row iff the caveat and/or
	// 	// context has changed. For ones in which the caveat name and/or context did not change, there is
	// 	// no need to replace the row, as it is already present.
	// 	for _, mut := range touchMutationsByNonCaveat {
	// 		deleteClauses = append(deleteClauses, exactRelationshipDifferentCaveatClause(mut.Tuple))
	// 	}
	// }

	// // Execute the DELETE operation for any DELETE mutations or TOUCH mutations that matched existing
	// // relationships and whose caveat name or context is different in some manner. We use RETURNING
	// // to determine which TOUCHed relationships were deleted by virtue of their caveat name and/or
	// // context being changed.
	// if len(deleteClauses) == 0 {
	// 	// Nothing more to do.
	// 	return nil
	// }
	//
	// builder := deleteTuple.
	// 	Where(deleteClauses).
	// 	Suffix(fmt.Sprintf("RETURNING %s, %s, %s, %s, %s, %s",
	// 		colNamespace,
	// 		colObjectID,
	// 		colRelation,
	// 		colUsersetNamespace,
	// 		colUsersetObjectID,
	// 		colUsersetRelation,
	// 	))
	//
	// sql, args, err := builder.
	// 	Set(colDeletedXid, rwt.newXID).
	// 	ToSql()
	// if err != nil {
	// 	return fmt.Errorf(errUnableToWriteRelationships, err)
	// }
	//
	// rows, err := rwt.tx.Query(ctx, sql, args...)
	// if err != nil {
	// 	return fmt.Errorf(errUnableToWriteRelationships, err)
	// }
	// defer rows.Close()
	//
	// // For each deleted row representing a TOUCH, recreate with the new caveat and/or context.
	// touchWrite := writeTuple
	// touchWriteHasValues := false
	//
	// deletedTpl := &core.RelationTuple{
	// 	ResourceAndRelation: &core.ObjectAndRelation{},
	// 	Subject:             &core.ObjectAndRelation{},
	// }
	//
	// for rows.Next() {
	// 	err := rows.Scan(
	// 		&deletedTpl.ResourceAndRelation.Namespace,
	// 		&deletedTpl.ResourceAndRelation.ObjectId,
	// 		&deletedTpl.ResourceAndRelation.Relation,
	// 		&deletedTpl.Subject.Namespace,
	// 		&deletedTpl.Subject.ObjectId,
	// 		&deletedTpl.Subject.Relation,
	// 	)
	// 	if err != nil {
	// 		return fmt.Errorf(errUnableToWriteRelationships, err)
	// 	}
	//
	// 	tplString := tuple.StringWithoutCaveat(deletedTpl)
	// 	mutation, ok := touchMutationsByNonCaveat[tplString]
	// 	if !ok {
	// 		// This did not represent a TOUCH operation.
	// 		continue
	// 	}
	//
	// 	touchWrite = appendForInsertion(touchWrite, mutation.Tuple)
	// 	touchWriteHasValues = true
	// }
	// rows.Close()
	//
	// // If no INSERTs are necessary to update caveats, then nothing more to do.
	// if !touchWriteHasValues {
	// 	return nil
	// }
	//
	// // Otherwise execute the INSERTs for the caveated-changes TOUCHed relationships.
	// sql, args, err = touchWrite.ToSql()
	// if err != nil {
	// 	return fmt.Errorf(errUnableToWriteRelationships, err)
	// }
	//
	// _, err = rwt.tx.Exec(ctx, sql, args...)
	// if err != nil {
	// 	return fmt.Errorf(errUnableToWriteRelationships, err)
	// }

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

func (rw *ydbReadWriter) selectTuples(
	ctx context.Context,
	in []*core.RelationTuple,
) ([]*core.RelationTuple, error) {
	span := trace.SpanFromContext(ctx)

	if len(in) == 0 {
		return nil, nil
	}

	var pred sq.Or
	for _, r := range in {
		pred = append(pred, exactRelationshipClause(r))
	}

	sql, args, err := rw.modifier(readRelationBuilder).View(ixUqRelationLiving).Where(pred).ToYdbSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	return queryTuples(ctx, rw.tablePathPrefix, sql, table.NewQueryParameters(args...), span, rw.executor)
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

func appendForInsertion(b sq.InsertBuilder, t *core.RelationTuple, rev revisions.TimestampRevision) sq.InsertBuilder {
	var caveatName *string
	var caveatContext *string
	if t.GetCaveat() != nil {
		caveatName = &t.GetCaveat().CaveatName
		if s := t.GetCaveat().GetContext().String(); len(s) > 0 {
			caveatContext = &s
		}
	}

	valuesToWrite := []interface{}{
		t.GetResourceAndRelation().GetNamespace(),
		t.GetResourceAndRelation().GetObjectId(),
		t.GetResourceAndRelation().GetRelation(),
		t.GetSubject().GetNamespace(),
		t.GetSubject().GetObjectId(),
		t.GetSubject().GetRelation(),
		caveatName,
		types.NullableJSONDocumentValue(caveatContext),
		rev.TimestampNanoSec(),
	}

	return b.Values(valuesToWrite...)
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
