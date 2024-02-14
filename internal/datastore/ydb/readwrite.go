package ydb

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type ydbReadWriter struct {
	*ydbReader
	executor    queryExecutor
	newRevision revisions.TimestampRevision
}

func (y *ydbReadWriter) WriteCaveats(ctx context.Context, definitions []*core.CaveatDefinition) error {
	// TODO implement me
	panic("implement me")
}

func (y *ydbReadWriter) DeleteCaveats(ctx context.Context, names []string) error {
	// TODO implement me
	panic("implement me")
}

func (y *ydbReadWriter) WriteRelationships(ctx context.Context, mutations []*core.RelationTupleUpdate) error {
	// TODO implement me
	panic("implement me")
}

func (y *ydbReadWriter) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter) error {
	// TODO implement me
	panic("implement me")
}

func (y *ydbReadWriter) WriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	// TODO implement me
	panic("implement me")
}

func (y *ydbReadWriter) DeleteNamespaces(ctx context.Context, nsNames ...string) error {
	// TODO implement me
	panic("implement me")
}

func (y *ydbReadWriter) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	// TODO implement me
	panic("implement me")
}
