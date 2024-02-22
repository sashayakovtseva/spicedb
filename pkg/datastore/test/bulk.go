package test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func BulkUploadTest(t *testing.T, tester DatastoreTester) {
	const ydbSelectLimit = 1000

	testCases := []int{0, 1, 10, 100, 1_000, 10_000}

	for _, tc := range testCases {
		t.Run(strconv.Itoa(tc), func(t *testing.T) {
			require := require.New(t)
			ctx := context.Background()

			rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
			require.NoError(err)
			t.Cleanup(func() { _ = rawDS.Close() })

			ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)
			bulkSource := testfixtures.NewBulkTupleGenerator(
				testfixtures.DocumentNS.Name,
				"viewer",
				testfixtures.UserNS.Name,
				tc,
				t,
			)

			_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				loaded, err := rwt.BulkLoad(ctx, bulkSource)
				require.NoError(err)
				require.Equal(uint64(tc), loaded)
				return err
			})
			require.NoError(err)

			tRequire := testfixtures.TupleChecker{Require: require, DS: ds}

			head, err := ds.HeadRevision(ctx)
			require.NoError(err)

			var (
				after       *core.RelationTuple
				isLastCheck bool
			)
			for left := tc; !isLastCheck; {
				if left == 0 {
					isLastCheck = true
				}

				iter, err := ds.SnapshotReader(head).QueryRelationships(ctx, datastore.RelationshipsFilter{
					ResourceType: testfixtures.DocumentNS.Name,
				},
					options.WithLimit(lo.ToPtr(uint64(ydbSelectLimit))),
					options.WithSort(options.ByResource),
					options.WithAfter(after),
				)
				require.NoError(err)

				expect := ydbSelectLimit
				if left < ydbSelectLimit {
					expect = left
				}

				tRequire.VerifyIteratorCount(iter, expect)

				if expect > 0 {
					after, err = iter.Cursor()
					require.NoError(err)
				}

				iter.Close()
				left -= expect
			}
		})
	}
}

func BulkUploadErrorsTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	ctx := context.Background()

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)
	t.Cleanup(func() { _ = rawDS.Close() })

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		inserted, err := rwt.BulkLoad(ctx, &onlyErrorSource{})

		// We can't check the specific error because pgx is not wrapping
		require.Error(err)
		require.Zero(inserted)
		return err
	})
	require.Error(err)
}

type onlyErrorSource struct{}

var errOnlyError = errors.New("source iterator error")

func (oes onlyErrorSource) Next(_ context.Context) (*core.RelationTuple, error) {
	return nil, errOnlyError
}

var _ datastore.BulkWriteRelationshipSource = onlyErrorSource{}
