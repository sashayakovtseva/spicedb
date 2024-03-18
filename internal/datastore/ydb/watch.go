package ydb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

func (y *ydbDatastore) Watch(
	ctx context.Context,
	afterRevision datastore.Revision,
	options datastore.WatchOptions,
) (<-chan *datastore.RevisionChanges, <-chan error) {
	watchBufferLength := options.WatchBufferLength
	if watchBufferLength <= 0 {
		watchBufferLength = y.config.watchBufferLength
	}

	updates := make(chan *datastore.RevisionChanges, watchBufferLength)
	errs := make(chan error, 1)

	features, err := y.Features(ctx)
	if err != nil {
		errs <- err
		return updates, errs
	}

	if !features.Watch.Enabled {
		errs <- datastore.NewWatchDisabledErr(features.Watch.Reason)
		return updates, errs
	}

	y.watchWg.Add(1)
	go y.watch(ctx, afterRevision, options, updates, errs)

	return updates, errs
}

type cdcEvent struct {
	Key      []cdcKeyElement
	NewImage json.RawMessage
}

type cdcKeyElement struct {
	String string
	Int64  int64
}

func (c *cdcKeyElement) UnmarshalJSON(in []byte) error {
	if len(in) == 0 {
		return fmt.Errorf("unexpected element len of 0")
	}

	if in[0] == '"' {
		c.String = string(in[1 : len(in)-1])
	} else {
		var err error
		c.Int64, err = strconv.ParseInt(string(in), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse int64 component: %w", err)
		}
	}

	return nil
}

type namespaceConfigImage struct {
	SerializedConfig  []byte `json:"serialized_config"`
	DeletedAtUnixNano *int64 `json:"deleted_at_unix_nano"`
}

type caveatImage struct {
	Definition        []byte `json:"definition"`
	DeletedAtUnixNano *int64 `json:"deleted_at_unix_nano"`
}

type relationTupleImage struct {
	CaveatName        *string `json:"caveat_name"`
	CaveatContext     *[]byte `json:"caveat_context"`
	DeletedAtUnixNano *int64  `json:"deleted_at_unix_nano"`
}

func (y *ydbDatastore) watch(
	ctx context.Context,
	afterRevision datastore.Revision,
	opts datastore.WatchOptions,
	updates chan *datastore.RevisionChanges,
	errs chan error,
) {
	defer y.watchWg.Done()
	defer close(updates)
	defer close(errs)

	afterTimestampRevision, ok := afterRevision.(revisions.TimestampRevision)
	if !ok {
		errs <- fmt.Errorf("expected timestamp revision, got  %T", afterRevision)
		return
	}
	readFromTime := afterTimestampRevision.Time()

	tableToTopicName := map[string]string{
		tableRelationTuple:   watchTopicName(y.config.tablePathPrefix, tableRelationTuple),
		tableCaveat:          watchTopicName(y.config.tablePathPrefix, tableCaveat),
		tableNamespaceConfig: watchTopicName(y.config.tablePathPrefix, tableNamespaceConfig),
	}
	topicToTableName := lo.Invert(tableToTopicName)

	topics := make([]string, 0, 3)
	if opts.Content&datastore.WatchRelationships == datastore.WatchRelationships {
		topics = append(topics, tableToTopicName[tableRelationTuple])
	}
	if opts.Content&datastore.WatchSchema == datastore.WatchSchema {
		topics = append(topics, tableToTopicName[tableNamespaceConfig])
		topics = append(topics, tableToTopicName[tableCaveat])
	}

	if len(topics) == 0 {
		errs <- fmt.Errorf("at least relationships or schema must be specified")
		return
	}

	sendError := func(err error) {
		if errors.Is(ctx.Err(), context.Canceled) {
			errs <- datastore.NewWatchCanceledErr()
			return
		}
		errs <- err
	}

	watchBufferWriteTimeout := opts.WatchBufferWriteTimeout
	if watchBufferWriteTimeout <= 0 {
		watchBufferWriteTimeout = y.config.watchBufferWriteTimeout
	}

	sendChange := func(change *datastore.RevisionChanges) bool {
		select {
		case updates <- change:
			return true
		default:
			// if we cannot immediately write, set up the timer and try again.
		}

		timer := time.NewTimer(watchBufferWriteTimeout)
		defer timer.Stop()

		select {
		case updates <- change:
			return true
		case <-timer.C:
			errs <- datastore.NewWatchDisconnectedErr()
			return false
		}
	}

	consumerUUID := uuid.New().String()
	log.Trace().
		Strs("topic", topics).
		Time("read_from", readFromTime).
		Str("consumer_uuid", consumerUUID).
		Msg("starting YDB reader")

	var selectors topicoptions.ReadSelectors
	for _, topic := range topics {
		selectors = append(selectors,
			topicoptions.ReadSelector{
				Path:     topic,
				ReadFrom: readFromTime,
			},
		)

		if err := y.driver.Topic().Alter(ctx, topic,
			topicoptions.AlterWithAddConsumers(topictypes.Consumer{
				Name:            consumerUUID,
				SupportedCodecs: []topictypes.Codec{topictypes.CodecRaw},
				ReadFrom:        readFromTime,
			}),
		); err != nil {
			sendError(fmt.Errorf("failed to create consumer: %w", err))
			return
		}
		defer func(topic string) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()

			err := y.driver.Topic().Alter(ctx, topic, topicoptions.AlterWithDropConsumers(consumerUUID))
			if err != nil {
				log.Warn().Str("topic", topic).Str("consumer", consumerUUID).Err(err).Msg("failed to remove consumer")
			}
		}(topic)
	}

	reader, err := y.driver.Topic().StartReader(
		consumerUUID,
		selectors,
		topicoptions.WithReaderCommitMode(topicoptions.CommitModeSync),
	)
	if err != nil {
		sendError(fmt.Errorf("failed to create reader: %w", err))
		return
	}
	defer func() { _ = reader.Close(ctx) }()

	if err := reader.WaitInit(ctx); err != nil {
		sendError(fmt.Errorf("failed to await reader initialization: %w", err))
		return
	}

	// WARNING! This is for test purpose only and only for namespace-aware schema watch!
	// todo Fix this when 'resolved'-like messages are supported.
	const expectedChangesCount = 4

	var (
		rErr    error
		msg     *topicreader.Message
		event   cdcEvent
		tracked = common.NewChanges(revisions.TimestampIDKeyFunc, opts.Content)

		changesCount    int
		changesRevision revisions.TimestampRevision
	)

	for msg, rErr = reader.ReadMessage(ctx); rErr == nil; msg, rErr = reader.ReadMessage(ctx) {
		if err := topicsugar.JSONUnmarshal(msg, &event); err != nil {
			sendError(fmt.Errorf("failed to unmarshal cdc event: %w", err))
			return
		}

		log.Trace().
			Str("topic", msg.Topic()).
			Int64("partition", msg.PartitionID()).
			Str("message_group_id", msg.MessageGroupID).
			Str("producer_id", msg.ProducerID).
			Int64("seq_no", msg.SeqNo).
			Int64("offset", msg.Offset).
			Time("created_at", msg.CreatedAt).
			Time("written_at", msg.WrittenAt).
			Any("metadata", msg.Metadata).
			Any("event", event).
			Msg("got new YDB CDC event")

		switch topicToTableName[msg.Topic()] {
		case tableRelationTuple:
			if len(event.Key) != 7 {
				sendError(spiceerrors.MustBugf("unexpected PK size. want 7, got %d (%v)", len(event.Key), event.Key))
				return
			}

			createdAtUnixNano := event.Key[6].Int64
			createRev := revisions.NewForTimestamp(createdAtUnixNano)

			tuple := &core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: event.Key[0].String,
					ObjectId:  event.Key[1].String,
					Relation:  event.Key[2].String,
				},
				Subject: &core.ObjectAndRelation{
					Namespace: event.Key[3].String,
					ObjectId:  event.Key[4].String,
					Relation:  event.Key[5].String,
				},
			}

			if event.NewImage == nil {
				break
			}

			var changes relationTupleImage
			if err := json.Unmarshal(event.NewImage, &changes); err != nil {
				sendError(fmt.Errorf("failed to unmarshal relation tuple new image: %w", err))
				return
			}

			var structuredCtx map[string]any
			if changes.CaveatContext != nil {
				if err := json.Unmarshal(*changes.CaveatContext, &structuredCtx); err != nil {
					sendError(fmt.Errorf("failed to unmarshal caveat context new image: %w", err))
					return
				}
			}

			ctxCaveat, err := common.ContextualizedCaveatFrom(lo.FromPtr(changes.CaveatName), structuredCtx)
			if err != nil {
				sendError(err)
				return
			}
			tuple.Caveat = ctxCaveat

			if changes.DeletedAtUnixNano != nil {
				deleteRev := revisions.NewForTimestamp(*changes.DeletedAtUnixNano)
				err := tracked.AddRelationshipChange(ctx, deleteRev, tuple, core.RelationTupleUpdate_DELETE)
				if err != nil {
					sendError(err)
					return
				}
				break
			}

			if err := tracked.AddRelationshipChange(ctx, createRev, tuple, core.RelationTupleUpdate_TOUCH); err != nil {
				sendError(err)
				return
			}

		case tableNamespaceConfig:
			if len(event.Key) != 2 {
				sendError(spiceerrors.MustBugf("unexpected PK size. want 2, got %d (%v)", len(event.Key), event.Key))
				return
			}

			namespaceName := event.Key[0].String
			createRev := revisions.NewForTimestamp(event.Key[1].Int64)

			if event.NewImage == nil {
				break
			}

			var changes namespaceConfigImage
			if err := json.Unmarshal(event.NewImage, &changes); err != nil {
				sendError(fmt.Errorf("failed to unmarshal namespace new image: %w", err))
				return
			}
			changesCount++
			changesRevision = createRev

			if changes.DeletedAtUnixNano != nil {
				deleteRev := revisions.NewForTimestamp(*changes.DeletedAtUnixNano)
				tracked.AddDeletedNamespace(ctx, deleteRev, namespaceName)
				break
			}

			var loaded core.NamespaceDefinition
			if err := loaded.UnmarshalVT(changes.SerializedConfig); err != nil {
				sendError(fmt.Errorf("failed to unmarshal namespace config: %w", err))
				return
			}
			tracked.AddChangedDefinition(ctx, createRev, &loaded)

		case tableCaveat:
			if len(event.Key) != 2 {
				sendError(spiceerrors.MustBugf("unexpected PK size. want 2, got %d (%v)", len(event.Key), event.Key))
				return
			}

			caveatName := event.Key[0].String
			createRev := revisions.NewForTimestamp(event.Key[1].Int64)

			if event.NewImage == nil {
				break
			}

			var changes caveatImage
			if err := json.Unmarshal(event.NewImage, &changes); err != nil {
				sendError(fmt.Errorf("failed to unmarshal caveat new image: %w", err))
				return
			}

			if changes.DeletedAtUnixNano != nil {
				deleteRev := revisions.NewForTimestamp(*changes.DeletedAtUnixNano)
				tracked.AddDeletedNamespace(ctx, deleteRev, caveatName)
				break
			}

			var loaded core.CaveatDefinition
			if err := loaded.UnmarshalVT(changes.Definition); err != nil {
				sendError(fmt.Errorf("failed to unmarshal caveat config: %w", err))
				return
			}
			tracked.AddChangedDefinition(ctx, createRev, &loaded)
		}

		if err := reader.Commit(ctx, msg); err != nil {
			sendError(fmt.Errorf("failed to commit offset: %w", err))
			return
		}

		if changesCount == expectedChangesCount {
			changes := tracked.AsRevisionChanges(revisions.TimestampIDKeyLessThanFunc)
			for i := range changes {
				if !sendChange(&changes[i]) {
					return
				}
			}

			// WARNING! This is for test purpose only and only for namespace-aware schema watch!
			// todo Fix this when 'resolved'-like messages are supported.
			futureCheckpoint := revisions.NewForTime(changesRevision.Time().Add(time.Hour * 24 * 30))
			if opts.Content&datastore.WatchCheckpoints == datastore.WatchCheckpoints {
				if !sendChange(&datastore.RevisionChanges{
					Revision:     futureCheckpoint,
					IsCheckpoint: true,
				}) {
					return
				}
			}

			changesCount = 0
			changesRevision = 0
			tracked = common.NewChanges(revisions.TimestampIDKeyFunc, opts.Content)
		}
	}

	if rErr != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			if err := reader.Close(closeCtx); err != nil {
				errs <- err
				return
			}
			errs <- datastore.NewWatchCanceledErr()
		} else {
			errs <- rErr
		}
	}
}

func watchTopicName(tablePathPrefix string, tableName string) string {
	topicName := tableName + "/" + changefeedSpicedbWatch
	if tablePathPrefix == "" {
		return topicName
	}
	return tablePathPrefix + "/" + topicName
}
