package ydb

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"golang.org/x/sync/errgroup"

	datastoreinternal "github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
)

func init() {
	datastore.Engines = append(datastore.Engines, Engine)
}

var (
	_ datastore.Datastore     = &ydbDatastore{}
	_ common.GarbageCollector = &ydbDatastore{}
)

const Engine = "ydb"

// NewYDBDatastore initializes a SpiceDB datastore that uses a YDB database.
func NewYDBDatastore(ctx context.Context, url string, options ...Option) (datastore.Datastore, error) {
	ds, err := newYDBDatastore(ctx, url, options...)
	if err != nil {
		return nil, err
	}
	return datastoreinternal.NewSeparatingContextDatastoreProxy(ds), nil
}

type ydbDatastore struct {
	driver *ydb.Driver

	gcGroup  *errgroup.Group
	gcCtx    context.Context
	cancelGc context.CancelFunc
	gcHasRun atomic.Bool
}

func newYDBDatastore(ctx context.Context, url string, options ...Option) (datastore.Datastore, error) {
	return nil, nil
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

func (y *ydbDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	// TODO implement me
	panic("implement me")
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

func (y *ydbDatastore) Features(ctx context.Context) (*datastore.Features, error) {
	return &datastore.Features{Watch: datastore.Feature{Enabled: true}}, nil
}

func (y *ydbDatastore) Close() error {
	y.cancelGc()

	if y.gcGroup != nil {
		if err := y.gcGroup.Wait(); err != nil {
			log.Warn().Err(err).Msg("failed to shutdown YDB garbage collector")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	if err := y.driver.Close(ctx); err != nil {
		log.Warn().Err(err).Msg("failed to shutdown YDB driver")
	}

	return nil
}
