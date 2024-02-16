package crdb

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"

	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
)

const (
	tableMetadata = "metadata"
	colUniqueID   = "unique_id"

	tableCounters = "relationship_estimate_counters"
	colID         = "id"
	colCount      = "count"
)

var (
	queryReadUniqueID         = psql.Select(colUniqueID).From(tableMetadata)
	queryRelationshipEstimate = fmt.Sprintf("SELECT COALESCE(SUM(%s), 0) FROM %s AS OF SYSTEM TIME follower_read_timestamp()", colCount, tableCounters)

	upsertCounterQuery = psql.Insert(tableCounters).Columns(
		colID,
		colCount,
	).Suffix(fmt.Sprintf("ON CONFLICT (%[1]s) DO UPDATE SET %[2]s = %[3]s.%[2]s + EXCLUDED.%[2]s RETURNING cluster_logical_timestamp()", colID, colCount, tableCounters))

	rng = rand.NewSource(time.Now().UnixNano())

	uniqueID string
)

func (cds *crdbDatastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	if len(uniqueID) == 0 {
		sql, args, err := queryReadUniqueID.ToSql()
		if err != nil {
			return datastore.Stats{}, fmt.Errorf("unable to prepare unique ID sql: %w", err)
		}
		if err := cds.readPool.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
			return row.Scan(&uniqueID)
		}, sql, args...); err != nil {
			return datastore.Stats{}, fmt.Errorf("unable to query unique ID: %w", err)
		}
	}

	var nsDefs []datastore.RevisionedNamespace
	var relCount int64

	if err := cds.readPool.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
		return row.Scan(&relCount)
	}, queryRelationshipEstimate); err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to read relationship count: %w", err)
	}

	if err := cds.readPool.BeginTxFunc(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()")
		if err != nil {
			return fmt.Errorf("unable to read namespaces: %w", err)
		}
		nsDefs, err = loadAllNamespaces(ctx, pgxcommon.QuerierFuncsFor(tx), func(sb sq.SelectBuilder, fromStr string) sq.SelectBuilder {
			return sb.From(fromStr)
		})
		if err != nil {
			return fmt.Errorf("unable to read namespaces: %w", err)
		}
		return nil
	}); err != nil {
		return datastore.Stats{}, err
	}

	// NOTE: this is a stop-gap solution to prevent panics in telemetry collection
	if relCount < 0 {
		relCount = 0
	}

	return datastore.Stats{
		UniqueID:                   uniqueID,
		EstimatedRelationshipCount: uint64(relCount),
		ObjectTypeStatistics:       datastore.ComputeObjectTypeStats(nsDefs),
	}, nil
}

func updateCounter(ctx context.Context, tx pgx.Tx, change int64) (datastore.Revision, error) {
	counterID := make([]byte, 2)
	_, err := rand.New(rng).Read(counterID)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf("unable to select random counter: %w", err)
	}

	sql, args, err := upsertCounterQuery.Values(counterID, change).ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf("unable to prepare upsert counter sql: %w", err)
	}

	var timestamp decimal.Decimal
	if err := tx.QueryRow(ctx, sql, args...).Scan(&timestamp); err != nil {
		return datastore.NoRevision, fmt.Errorf("unable to executed upsert counter query: %w", err)
	}

	return revisions.NewForHLC(timestamp)
}
