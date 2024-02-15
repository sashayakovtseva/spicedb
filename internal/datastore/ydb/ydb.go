package ydb

import (
	"context"
	"fmt"
	"time"

	ydbOtel "github.com/ydb-platform/ydb-go-sdk-otel"
	ydbZerolog "github.com/ydb-platform/ydb-go-sdk-zerolog"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	datastoreinternal "github.com/authzed/spicedb/internal/datastore"
	datastoreCommon "github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/internal/datastore/ydb/common"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/truetime"
)

func init() {
	datastore.Engines = append(datastore.Engines, Engine)
}

var (
	_ datastore.Datastore              = &ydbDatastore{}
	_ datastoreCommon.GarbageCollector = &ydbDatastore{}
)

const Engine = "ydb"

// NewYDBDatastore initializes a SpiceDB datastore that uses a YDB database.
func NewYDBDatastore(ctx context.Context, dsn string, opts ...Option) (datastore.Datastore, error) {
	ds, err := newYDBDatastore(ctx, dsn, opts...)
	if err != nil {
		return nil, err
	}
	return datastoreinternal.NewSeparatingContextDatastoreProxy(ds), nil
}

type ydbDatastore struct {
	driver *ydb.Driver
	config ydbConfig
}

func newYDBDatastore(ctx context.Context, dsn string, opts ...Option) (*ydbDatastore, error) {
	parsedDSN := common.ParseDSN(dsn)

	config := generateConfig(opts)
	config.tablePathPrefix = parsedDSN.TablePathPrefix

	db, err := ydb.Open(
		ctx,
		parsedDSN.OriginalDSN,
		ydbZerolog.WithTraces(&log.Logger, trace.DatabaseSQLEvents),
		ydbOtel.WithTraces(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open YDB connectionn: %w", err)
	}

	return &ydbDatastore{
		driver: db,
		config: config,
	}, nil
}

func (y *ydbDatastore) SnapshotReader(revision datastore.Revision) datastore.Reader {
	// TODO implement me
	panic("implement me")
}

func (y *ydbDatastore) ReadWriteTx(
	ctx context.Context,
	fn datastore.TxUserFunc,
	opts ...options.RWTOptionsOption,
) (datastore.Revision, error) {

	// TODO implement me
	panic("implement me")
}

func (y *ydbDatastore) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	// TODO implement me
	panic("implement me")
}

func (y *ydbDatastore) HeadRevision(_ context.Context) (datastore.Revision, error) {
	now := truetime.UnixNano()
	return revisions.NewForTimestamp(now), nil
}

func (y *ydbDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	// TODO implement me
	panic("implement me")
}

func (y *ydbDatastore) RevisionFromString(serialized string) (datastore.Revision, error) {
	// TODO implement me
	panic("implement me")
}

func (y *ydbDatastore) Watch(ctx context.Context, afterRevision datastore.Revision, options datastore.WatchOptions) (<-chan *datastore.RevisionChanges, <-chan error) {
	// TODO implement me
	panic("implement me")
}

func (y *ydbDatastore) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	// TODO implement me
	panic("implement me")
}

func (y *ydbDatastore) Features(_ context.Context) (*datastore.Features, error) {
	return &datastore.Features{Watch: datastore.Feature{Enabled: true}}, nil
}

func (y *ydbDatastore) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	if err := y.driver.Close(ctx); err != nil {
		log.Warn().Err(err).Msg("failed to shutdown YDB driver")
	}

	return nil
}

func queryRowTx(
	ctx context.Context,
	tx table.TransactionActor,
	query string,
	queryParams *table.QueryParameters,
	values ...indexed.RequiredOrOptional,
) error {
	res, err := tx.Execute(
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
