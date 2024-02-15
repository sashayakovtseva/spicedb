package ydb

import (
	"context"
	"fmt"
	"time"

	ydbOtel "github.com/ydb-platform/ydb-go-sdk-otel"
	ydbZerolog "github.com/ydb-platform/ydb-go-sdk-zerolog"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.opentelemetry.io/otel"

	datastoreinternal "github.com/authzed/spicedb/internal/datastore"
	datastoreCommon "github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/internal/datastore/ydb/common"
	"github.com/authzed/spicedb/internal/datastore/ydb/migrations"
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

	ParseRevisionString = revisions.RevisionParser(revisions.Timestamp)

	tracer = otel.Tracer("spicedb/internal/datastore/ydb")
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
	*revisions.RemoteClockRevisions
	revisions.CommonDecoder

	driver *ydb.Driver
	config *ydbConfig

	originalDSN string
}

func newYDBDatastore(ctx context.Context, dsn string, opts ...Option) (*ydbDatastore, error) {
	parsedDSN := common.ParseDSN(dsn)

	config := generateConfig(opts)
	if parsedDSN.TablePathPrefix != "" {
		config.tablePathPrefix = parsedDSN.TablePathPrefix
	}

	db, err := ydb.Open(
		ctx,
		parsedDSN.OriginalDSN,
		ydbZerolog.WithTraces(&log.Logger, trace.DatabaseSQLEvents),
		ydbOtel.WithTraces(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open YDB connectionn: %w", err)
	}

	maxRevisionStaleness := time.Duration(float64(config.revisionQuantization.Nanoseconds())*
		config.maxRevisionStalenessPercent) * time.Nanosecond

	ds := &ydbDatastore{
		RemoteClockRevisions: revisions.NewRemoteClockRevisions(
			config.gcWindow,
			maxRevisionStaleness,
			config.followerReadDelay,
			config.revisionQuantization,
		),
		CommonDecoder: revisions.CommonDecoder{Kind: revisions.Timestamp},

		driver:      db,
		originalDSN: dsn,
		config:      config,
	}
	ds.SetNowFunc(ds.HeadRevision)

	return ds, nil
}

func (y *ydbDatastore) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	if err := y.driver.Close(ctx); err != nil {
		log.Warn().Err(err).Msg("failed to shutdown YDB driver")
	}

	return nil
}

func (y *ydbDatastore) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	headMigration, err := migrations.YDBMigrations.HeadRevision()
	if err != nil {
		return datastore.ReadyState{}, fmt.Errorf("invalid head migration found for YDB: %w", err)
	}

	ydbDriver, err := migrations.NewYDBDriver(ctx, y.originalDSN)
	if err != nil {
		return datastore.ReadyState{}, err
	}
	defer ydbDriver.Close(ctx)

	version, err := ydbDriver.Version(ctx)
	if err != nil {
		return datastore.ReadyState{}, err
	}

	if version != headMigration {
		return datastore.ReadyState{
			Message: fmt.Sprintf(
				"YDB is not migrated: currently at revision `%s`, but requires `%s`. Please run `spicedb migrate`.",
				version,
				headMigration,
			),
		}, nil
	}

	return datastore.ReadyState{IsReady: true}, nil
}

func (y *ydbDatastore) Features(_ context.Context) (*datastore.Features, error) {
	return &datastore.Features{Watch: datastore.Feature{Enabled: true}}, nil
}

func (y *ydbDatastore) SnapshotReader(revision datastore.Revision) datastore.Reader {
	rev := revision.(revisions.TimestampRevision)

	return newYDBReader(
		y.config.tablePathPrefix,
		sessionQueryExecutor{driver: y.driver},
		revisionedQueryModifier(rev),
	)
}

// ReadWriteTx starts a read/write transaction, which will be committed if no error is
// returned and rolled back if an error is returned.
// Important note from YDB docs:
//
//	All changes made during the transaction accumulate in the database server memory
//	and are applied when the transaction completes. If the locks are not invalidated,
//	all the changes accumulated are committed atomically; if at least one lock is
//	invalidated, none of the changes committed. The above model involves certain
//	restrictions: changes made by a single transaction must fit inside available
//	memory, and a transaction "doesn't see" its changes.
//
// todo verify all ReadWriteTx usages order reads before writes.
func (y *ydbDatastore) ReadWriteTx(
	ctx context.Context,
	fn datastore.TxUserFunc,
	opts ...options.RWTOptionsOption,
) (datastore.Revision, error) {
	config := options.NewRWTOptionsWithOptions(opts...)

	txOptions := []table.Option{
		table.WithTxSettings(table.TxSettings(table.WithSerializableReadWrite())),
	}
	if !config.DisableRetries {
		txOptions = append(txOptions, table.WithIdempotent())
	}

	// todo use maxRetries somehow.
	var newRev revisions.TimestampRevision
	err := y.driver.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		// this is actually a BAD way to do mvcc which may lead to new enemy problem.
		// we MUST choose revision at the moment of commit,
		// but YDB is not Spanner, so this is the best effort possible.
		now := truetime.UnixNano()
		newRev = revisions.NewForTimestamp(now)

		rw := &ydbReadWriter{
			ydbReader:   newYDBReader(y.config.tablePathPrefix, tx, livingObjectModifier),
			newRevision: newRev,
		}

		return fn(ctx, rw)
	}, txOptions...)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf("failed to process transaction: %w", err)
	}
	return newRev, nil
}

func (y *ydbDatastore) HeadRevision(_ context.Context) (datastore.Revision, error) {
	now := truetime.UnixNano()
	return revisions.NewForTimestamp(now), nil
}

func (y *ydbDatastore) Watch(ctx context.Context, afterRevision datastore.Revision, options datastore.WatchOptions) (<-chan *datastore.RevisionChanges, <-chan error) {
	// TODO implement me
	panic("implement me")
}
