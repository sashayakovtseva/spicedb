package ydb

import (
	"context"
	"time"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
)

func (y *ydbDatastore) HasGCRun() bool {
	// TODO implement me
	panic("implement me")
}

func (y *ydbDatastore) MarkGCCompleted() {
	// TODO implement me
	panic("implement me")
}

func (y *ydbDatastore) ResetGCCompleted() {
	// TODO implement me
	panic("implement me")
}

func (y *ydbDatastore) Now(ctx context.Context) (time.Time, error) {
	// TODO implement me
	panic("implement me")
}

func (y *ydbDatastore) TxIDBefore(ctx context.Context, time time.Time) (datastore.Revision, error) {
	// TODO implement me
	panic("implement me")
}

func (y *ydbDatastore) DeleteBeforeTx(ctx context.Context, txID datastore.Revision) (common.DeletionCounts, error) {
	// TODO implement me
	panic("implement me")
}
