package ydb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/prometheus/client_golang/prometheus"
	ydbOtel "github.com/ydb-platform/ydb-go-sdk-otel"
	ydbPrometheus "github.com/ydb-platform/ydb-go-sdk-prometheus"
	ydbZerolog "github.com/ydb-platform/ydb-go-sdk-zerolog"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.opentelemetry.io/otel"

	datastoreinternal "github.com/authzed/spicedb/internal/datastore"
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
	_ datastore.Datastore = (*ydbDatastore)(nil)

	ParseRevisionString = revisions.RevisionParser(revisions.Timestamp)

	yq     = sq.StatementBuilder.PlaceholderFormat(sq.DollarP)
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

	closeOnce sync.Once
	// isClosed used in HeadRevision only to pass datastore tests.
	isClosed atomic.Bool
	watchWg  sync.WaitGroup
}

func newYDBDatastore(ctx context.Context, dsn string, opts ...Option) (*ydbDatastore, error) {
	parsedDSN := common.ParseDSN(dsn)

	config := generateConfig(opts)
	if parsedDSN.TablePathPrefix != "" {
		config.tablePathPrefix = parsedDSN.TablePathPrefix
	}

	ydbOpts := []ydb.Option{
		ydbZerolog.WithTraces(&log.Logger, trace.DatabaseSQLEvents),
		ydbOtel.WithTraces(),
	}
	if config.enablePrometheusStats {
		ydbOpts = append(ydbOpts, ydbPrometheus.WithTraces(prometheus.DefaultRegisterer))
	}
	if config.certificatePath != "" {
		ydbOpts = append(ydbOpts, ydb.WithCertificatesFromFile(config.certificatePath))
	}

	db, err := ydb.Open(ctx, parsedDSN.OriginalDSN, ydbOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to open YDB connection: %w", err)
	}

	if _, err := db.Scheme().ListDirectory(ctx, db.Name()); err != nil {
		return nil, fmt.Errorf("failed to ping YDB: %w", err)
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
	if y.isClosed.Load() {
		return nil
	}

	var err error
	y.closeOnce.Do(func() {
		y.watchWg.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
		defer cancel()

		err = y.driver.Close(ctx)
		y.isClosed.Store(true)
	})

	return err
}

func (y *ydbDatastore) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	headMigration, err := migrations.YDBMigrations.HeadRevision()
	if err != nil {
		return datastore.ReadyState{}, fmt.Errorf("invalid head migration found for YDB: %w", err)
	}

	ydbDriver, err := migrations.NewYDBDriver(
		ctx,
		y.originalDSN,
		migrations.WithTablePathPrefix(y.config.tablePathPrefix),
		migrations.WithCertificatePath(y.config.certificatePath),
	)
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
		true,
	)
}

// ReadWriteTx starts a read/write transaction, which will be committed if
// no error is returned and rolled back if an error is returned.
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

	var newRev revisions.TimestampRevision
	err := y.driver.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		// this is actually a BAD way to do mvcc which may lead to new enemy problem.
		// we MUST choose revision at the moment of commit,
		// but YDB is not Spanner, so this is the best effort possible.
		now := truetime.UnixNano()
		newRev = revisions.NewForTimestamp(now)

		rw := &ydbReadWriter{
			ydbReader: newYDBReader(
				y.config.tablePathPrefix,
				txQueryExecutor{tx: tx},
				livingObjectModifier,
				false,
			),
			bulkLoadBatchSize:     y.config.bulkLoadBatchSize,
			newRevision:           newRev,
			enableUniquenessCheck: y.config.enableUniquenessCheck,
		}

		return fn(ctx, rw)
	}, txOptions...)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf("failed to process transaction: %w", err)
	}
	return newRev, nil
}

func (y *ydbDatastore) HeadRevision(_ context.Context) (datastore.Revision, error) {
	if y.isClosed.Load() {
		return datastore.NoRevision, fmt.Errorf("datastore is closed")
	}

	now := truetime.UnixNano()
	return revisions.NewForTimestamp(now), nil
}
