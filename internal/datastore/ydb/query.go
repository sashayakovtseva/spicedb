package ydb

import (
	"context"
	"fmt"

	yq "github.com/flymedllva/ydb-go-qb/yqb"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"
)

type queryModifier func(yq.SelectBuilder) yq.SelectBuilder

type queryExecutor interface {
	Execute(
		ctx context.Context,
		query string,
		params *table.QueryParameters,
		opts ...options.ExecuteDataQueryOption,
	) (result.Result, error)
}

func queryRowTx(
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
