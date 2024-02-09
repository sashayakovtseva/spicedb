package ydb

import (
	"context"
	"fmt"

	yq "github.com/flymedllva/ydb-go-qb/yqb"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"

	"github.com/authzed/spicedb/internal/datastore/revisions"
)

const (
	colCreatedAtUnixNano = "created_at_unix_nano"
	colDeletedAtUnixNano = "deleted_at_unix_nano"

	// namespace_config
	tableNamespaceConfig = "namespace_config"
	colSerializedConfig  = "serialized_config"
	colNamespace         = "namespace"
)

var (
	livingObjectModifier = queryModifier(func(builder yq.SelectBuilder) yq.SelectBuilder {
		return builder.Where(yq.Eq{colDeletedAtUnixNano: nil})
	})
)

type queryModifier func(yq.SelectBuilder) yq.SelectBuilder

func revisionedQueryModifier(revision revisions.TimestampRevision) queryModifier {
	return func(builder yq.SelectBuilder) yq.SelectBuilder {
		return builder.
			Where(yq.LtOrEq{colCreatedAtUnixNano: revision.TimestampNanoSec()}).
			Where(yq.Or{
				yq.Eq{colDeletedAtUnixNano: nil},
				yq.Gt{colDeletedAtUnixNano: revision.TimestampNanoSec()},
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
