package ydb

import (
	"context"
	"encoding/json"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/samber/lo"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	ydbCommon "github.com/authzed/spicedb/internal/datastore/ydb/common"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	// common
	colCreatedAtUnixNano = "created_at_unix_nano"
	colDeletedAtUnixNano = "deleted_at_unix_nano"
	colNamespace         = "namespace"

	// namespace_config
	tableNamespaceConfig = "namespace_config"
	colSerializedConfig  = "serialized_config"
	ixUqNamespaceLiving  = "uq_namespace_living"

	// caveat
	tableCaveat      = "caveat"
	colName          = "name"
	colDefinition    = "definition"
	ixUqCaveatLiving = "uq_caveat_living"

	// relation_tuple
	tableRelationTuple  = "relation_tuple"
	colObjectID         = "object_id"
	colRelation         = "relation"
	colUsersetNamespace = "userset_namespace"
	colUsersetObjectID  = "userset_object_id"
	colUsersetRelation  = "userset_relation"
	colCaveatName       = "caveat_name"
	colCaveatContext    = "caveat_context"
)

var (
	relationTupleSchema = common.NewSchemaInformation(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCaveatName,
		common.TupleComparison,
	)

	livingObjectPredicate = sq.Eq{colDeletedAtUnixNano: nil}
	livingObjectModifier  = queryModifier(func(builder sq.SelectBuilder) sq.SelectBuilder {
		return builder.Where(livingObjectPredicate)
	})

	readCaveatBuilder   = sq.Select(colDefinition, colCreatedAtUnixNano).From(tableCaveat)
	deleteCaveatBuilder = sq.Update(tableCaveat).Where(livingObjectPredicate)
	insertCaveatBuilder = sq.Insert(tableCaveat).Columns(colName, colDefinition, colCreatedAtUnixNano)

	readNamespaceBuilder   = sq.Select(colSerializedConfig, colCreatedAtUnixNano).From(tableNamespaceConfig)
	deleteNamespaceBuilder = sq.Update(tableNamespaceConfig).Where(livingObjectModifier)
	insertNamespaceBuilder = sq.Insert(tableNamespaceConfig).
				Columns(colNamespace, colSerializedConfig, colCreatedAtUnixNano)

	readRelationBuilder = sq.Select(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCaveatName,
		colCaveatContext,
	).From(tableRelationTuple)
	deleteRelationBuilder = sq.Update(tableRelationTuple).Where(livingObjectModifier)
)

type queryModifier func(sq.SelectBuilder) sq.SelectBuilder

func revisionedQueryModifier(revision revisions.TimestampRevision) queryModifier {
	return func(builder sq.SelectBuilder) sq.SelectBuilder {
		return builder.
			Where(sq.LtOrEq{colCreatedAtUnixNano: revision.TimestampNanoSec()}).
			Where(sq.Or{
				sq.Eq{colDeletedAtUnixNano: nil},
				sq.Gt{colDeletedAtUnixNano: revision.TimestampNanoSec()},
			})
	}
}

type queryExecutor interface {
	Execute(
		ctx context.Context,
		query string,
		params *table.QueryParameters,
		opts ...options.ExecuteDataQueryOption,
	) (result.Result, error)
}

// sessionQueryExecutor implements queryExecutor for YDB sessions
// with online read-only auto commit transaction mode.
type sessionQueryExecutor struct {
	driver *ydb.Driver
}

func (se sessionQueryExecutor) Execute(
	ctx context.Context,
	query string,
	params *table.QueryParameters,
	opts ...options.ExecuteDataQueryOption,
) (result.Result, error) {
	// todo check whether it is ok to close result outside of Do busy loop.
	var res result.Result
	err := se.driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		var err error
		_, res, err = s.Execute(ctx, table.OnlineReadOnlyTxControl(), query, params, opts...)
		return err
	}, table.WithIdempotent())
	return res, err
}

func queryRow(
	ctx context.Context,
	executor queryExecutor,
	query string,
	queryParams *table.QueryParameters,
	values ...indexed.RequiredOrOptional,
) error {
	res, err := executor.Execute(
		ctx,
		query,
		queryParams,
	)
	if err != nil {
		return err
	}
	defer res.Close()

	if err := res.NextResultSetErr(ctx); err != nil {
		return err
	}
	if !res.NextRow() {
		return fmt.Errorf("no unique id rows")
	}
	if err := res.Scan(values...); err != nil {
		return err
	}
	if err := res.Err(); err != nil {
		return err
	}

	return nil
}

func toYQLWrapper(b sq.SelectBuilder) (string, []any, error) {
	query, yqlParams, err := b.ToYdbSql()
	if err != nil {
		return "", nil, err
	}

	// todo think how to get rid of this at all.
	genericArgs := make([]any, len(yqlParams))
	for i := range yqlParams {
		genericArgs[i] = yqlParams[i]
	}

	return query, genericArgs, nil
}

func newYDBCommonQueryExecutor(tablePathPrefix string, ydbExecutor queryExecutor) common.ExecuteQueryFunc {
	return func(ctx context.Context, sql string, args []any) ([]*corev1.RelationTuple, error) {
		span := trace.SpanFromContext(ctx)
		return queryTuples(ctx, tablePathPrefix, sql, args, span, ydbExecutor)
	}
}

// queryTuples queries tuples for the given query and transaction.
func queryTuples(
	ctx context.Context,
	tablePathPrefix string,
	query string,
	args []any,
	span trace.Span,
	ydbExecutor queryExecutor,
) ([]*corev1.RelationTuple, error) {
	params := table.NewQueryParameters()
	for _, a := range args {
		params.Add(a.(table.ParameterOption))
	}

	query = ydbCommon.AddTablePrefix(query, tablePathPrefix)
	res, err := ydbExecutor.Execute(ctx, query, params)
	if err != nil {
		return nil, fmt.Errorf("failed to execute relation tuples query: %w", err)
	}
	defer res.Close()

	span.AddEvent("Query issued to database")

	var tuples []*corev1.RelationTuple
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			nextTuple := &corev1.RelationTuple{
				ResourceAndRelation: &corev1.ObjectAndRelation{},
				Subject:             &corev1.ObjectAndRelation{},
			}
			var caveatName *string
			var caveatCtx *[]byte
			err := res.Scan(
				&nextTuple.ResourceAndRelation.Namespace,
				&nextTuple.ResourceAndRelation.ObjectId,
				&nextTuple.ResourceAndRelation.Relation,
				&nextTuple.Subject.Namespace,
				&nextTuple.Subject.ObjectId,
				&nextTuple.Subject.Relation,
				&caveatName,
				&caveatCtx,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to scan relation tuple: %w", err)
			}

			var structuredCtx map[string]any
			if caveatCtx != nil {
				if err := json.Unmarshal(*caveatCtx, &structuredCtx); err != nil {
					return nil, fmt.Errorf("failed to unmarhsla relation tuple caveat context: %w", err)
				}
			}

			nextTuple.Caveat, err = common.ContextualizedCaveatFrom(lo.FromPtr(caveatName), structuredCtx)
			if err != nil {
				return nil, fmt.Errorf("failed to create relation tuple caveat: %w", err)
			}
			tuples = append(tuples, nextTuple)
		}
	}

	if err := res.Err(); err != nil {
		return nil, fmt.Errorf("failed to read relation tuples: %w", err)
	}

	span.AddEvent("Tuples loaded", trace.WithAttributes(attribute.Int("tupleCount", len(tuples))))
	return tuples, nil
}

func executeDeleteQuery(
	ctx context.Context,
	tablePathPrefix string,
	executor queryExecutor,
	b sq.UpdateBuilder,
	deleteRev revisions.TimestampRevision,
	pred sq.Sqlizer,
) error {
	sql, args, err := b.Set(colDeletedAtUnixNano, deleteRev.TimestampNanoSec()).Where(pred).ToYdbSql()
	if err != nil {
		return fmt.Errorf("failed to build query: %w", err)
	}

	sql = ydbCommon.AddTablePrefix(sql, tablePathPrefix)
	res, err := executor.Execute(ctx, sql, table.NewQueryParameters(args...))
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer res.Close()

	return nil
}
