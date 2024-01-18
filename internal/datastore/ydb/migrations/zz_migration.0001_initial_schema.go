package migrations

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

// YDB doesn't support unique secondary indexes, so one should manually check row uniqueness before insert.
// Suggested way to do so is to use
//
//	DISCARD SELECT Ensure(0, false, "duplicate") FROM table VIEW index WHERE id=$Id
//
// For convenience, secondary indexes for uniqueness check start with `uq_` prefix.
// YDB also doesn't support partial secondary indexes.
// Table's PK columns are always implicitly saved in secondary index as well.
const (
	// todo add NOT NULL created_at_unix_nano.
	createSchemaVersion = `
CREATE TABLE schema_version (
	version_num Utf8 NOT NULL,
	created_at_unix_nano Int64,
	PRIMARY KEY (version_num)
);`

	createUniqueIDTable = `
CREATE TABLE metadata (
	unique_id String NOT NULL,
	PRIMARY KEY (unique_id)
);`

	// todo add NOT NULL serialized_config.
	// todo check namespace name with Ensure in insert.
	// todo AUTO_PARTITIONING_BY_LOAD?
	createNamespaceConfig = `
CREATE TABLE namespace_config (
	namespace Utf8 NOT NULL,
	serialized_config String,
	created_at_unix_nano Int64 NOT NULL,
	deleted_at_unix_nano Int64,
	PRIMARY KEY (namespace, created_at_unix_nano, deleted_at_unix_nano),
	INDEX uq_namespace_living GLOBAL SYNC ON (namespace, deleted_at_unix_nano)
);`

	// todo add NOT NULL definition.
	// todo AUTO_PARTITIONING_BY_LOAD?
	createCaveat = `
CREATE TABLE caveat (
	name Utf8 NOT NULL,
	definition String,
	created_at_unix_nano Int64 NOT NULL,
	deleted_at_unix_nano Int64,
	PRIMARY KEY (name, created_at_unix_nano, deleted_at_unix_nano),
	INDEX uq_caveat_living GLOBAL SYNC ON (name, deleted_at_unix_nano)
);`

	// todo discuss JsonDocument instead of Json.
	// todo check Ensure on insert.
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
	PRIMARY KEY (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation, created_at_unix_nano, deleted_at_unix_nano),
	INDEX uq_relation_tuple_living GLOBAL SYNC ON (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation, deleted_at_unix_nano),
	INDEX ix_relation_tuple_by_subject GLOBAL SYNC ON (userset_object_id, userset_namespace, userset_relation, namespace, relation),
	INDEX ix_relation_tuple_by_subject_relation GLOBAL SYNC ON (userset_namespace, userset_relation, namespace, relation),
	INDEX ix_relation_tuple_alive_by_resource_rel_subject_covering GLOBAL SYNC ON (namespace, relation, userset_namespace) COVER (caveat_name, caveat_context),
	INDEX ix_gc_index GLOBAL SYNC ON (deleted_at_unix_nano)
);`

	insertUniqueID = `INSERT INTO metadata (unique_id) VALUES (CAST(RandomUuid(1) as String));`
)

func init() {
	err := YDBMigrations.Register("initial", "", func(ctx context.Context, client table.Client) error {
		return client.Do(ctx, func(ctx context.Context, s table.Session) error {
			statements := []string{
				createSchemaVersion,
				createUniqueIDTable,
				createNamespaceConfig,
				createCaveat,
				createRelationTuple,
			}
			for _, stmt := range statements {
				if err := s.ExecuteSchemeQuery(ctx, stmt); err != nil {
					return err
				}
			}

			return nil
		})
	}, func(ctx context.Context, tx table.TransactionActor) error {
		_, err := tx.Execute(ctx, insertUniqueID, &table.QueryParameters{})
		return err
	})
	if err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
