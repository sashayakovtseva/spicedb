package migrations

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"github.com/authzed/spicedb/internal/datastore/ydb/common"
)

// YDB doesn't support unique secondary indexes, so one should manually check row uniqueness before insert.
// Suggested way to do so is to use
//
//	DISCARD SELECT Ensure(0, false, "duplicate") FROM table VIEW index WHERE id=$Id
//
// For convenience, secondary indexes that are suitable for uniqueness check start with `uq_` prefix.
//
// YDB also doesn't support partial secondary indexes.
// Table's PK columns are always implicitly saved in secondary index as well.
//
// YDB also doesn't support PK update. Given that, PK differ a lot from other datastore implementations.
//
// YDB doesn't support automatic secondary index selection, so one should
// use SELECT VIEW to enable reasonable query time and eliminate full scans.
const (
	createSchemaVersion = `
CREATE TABLE schema_version (
	version_num Utf8 NOT NULL,
	created_at_unix_nano Int64 NOT NULL,
	PRIMARY KEY (version_num)
);`

	createUniqueIDTable = `
CREATE TABLE metadata (
	unique_id String NOT NULL,
	PRIMARY KEY (unique_id)
);`

	// todo AUTO_PARTITIONING_BY_LOAD?
	// ideally PK should be (namespace, deleted_at_unix_nano), but since deleted_at_unix_nano is
	// updated during delete operation it cannot be used. simply (namespace) is also not applicable
	// b/c there might be deleted namespaces with the same name as currently living.
	// uq_namespace_living columns order is determined by list namespaces queries.
	createNamespaceConfig = `
CREATE TABLE namespace_config (
	namespace Utf8 NOT NULL,
	serialized_config String NOT NULL,
	created_at_unix_nano Int64 NOT NULL,
	deleted_at_unix_nano Int64,
	PRIMARY KEY (namespace, created_at_unix_nano),
	INDEX uq_namespace_living GLOBAL SYNC ON (deleted_at_unix_nano, namespace) COVER (serialized_config)
);`

	// todo AUTO_PARTITIONING_BY_LOAD?
	// ideally PK should be (name, deleted_at_unix_nano), but since deleted_at_unix_nano is
	// updated during delete operation it cannot be used. simply (name) is also not applicable
	// b/c there might be deleted caveats with the same name as currently living.
	// uq_caveat_living columns order is determined by list caveats queries.
	createCaveat = `
CREATE TABLE caveat (
	name Utf8 NOT NULL,
	definition String NOT NULL,
	created_at_unix_nano Int64 NOT NULL,
	deleted_at_unix_nano Int64,
	PRIMARY KEY (name, created_at_unix_nano),
	INDEX uq_caveat_living GLOBAL SYNC ON (deleted_at_unix_nano, name) COVER (definition)
);`

	// todo discuss JsonDocument instead of Json.
	// todo check Ensure on insert, check indexed.
	// todo AUTO_PARTITIONING_BY_LOAD?
	createRelationTuple = `
CREATE TABLE relation_tuple (
	namespace Utf8 NOT NULL,
	object_id Utf8 NOT NULL,
	relation Utf8 NOT NULL,
	userset_namespace Utf8 NOT NULL,
	userset_object_id Utf8 NOT NULL,
	userset_relation Utf8 NOT NULL,
	caveat_name Utf8,
	caveat_context JsonDocument,
	created_at_unix_nano Int64 NOT NULL,
	deleted_at_unix_nano Int64,
	PRIMARY KEY (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation, created_at_unix_nano),
	INDEX uq_relation_tuple_living GLOBAL SYNC ON (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation, deleted_at_unix_nano),
	INDEX ix_relation_tuple_by_subject GLOBAL SYNC ON (userset_object_id, userset_namespace, userset_relation, namespace, relation),
	INDEX ix_relation_tuple_by_subject_relation GLOBAL SYNC ON (userset_namespace, userset_relation, namespace, relation),
	INDEX ix_relation_tuple_alive_by_resource_rel_subject_covering GLOBAL SYNC ON (namespace, relation, userset_namespace) COVER (caveat_name, caveat_context),
	INDEX ix_gc_index GLOBAL SYNC ON (deleted_at_unix_nano)
);`

	insertUniqueID = `INSERT INTO metadata (unique_id) VALUES (CAST(RandomUuid(1) as String));`
)

func init() {
	err := YDBMigrations.Register("initial", "", func(ctx context.Context, client TableClientWithOptions) error {
		return client.client.Do(ctx, func(ctx context.Context, s table.Session) error {
			statements := []string{
				common.AddTablePrefix(createSchemaVersion, client.opts.tablePathPrefix),
				common.AddTablePrefix(createUniqueIDTable, client.opts.tablePathPrefix),
				common.AddTablePrefix(createNamespaceConfig, client.opts.tablePathPrefix),
				common.AddTablePrefix(createCaveat, client.opts.tablePathPrefix),
				common.AddTablePrefix(createRelationTuple, client.opts.tablePathPrefix),
			}
			for _, stmt := range statements {
				if err := s.ExecuteSchemeQuery(ctx, stmt); err != nil {
					return err
				}
			}

			return nil
		})
	}, func(ctx context.Context, tx TxActorWithOptions) error {
		_, err := tx.tx.Execute(
			ctx,
			common.AddTablePrefix(insertUniqueID, tx.opts.tablePathPrefix),
			&table.QueryParameters{},
		)
		return err
	})
	if err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
