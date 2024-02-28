package ydb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
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

	go y.watch(ctx, afterRevision, options, updates, errs)

	return updates, errs
}

type cdcEvent struct {
	Key      []string
	NewImage json.RawMessage
}

type namespaceConfigImage struct {
	SerializedConfig  []byte
	DeletedAtUnixNano *int64
}

type caveatImage struct {
	Definition        []byte
	DeletedAtUnixNano *int64
}

type relationTupleImage struct {
	CaveatName        *string
	CaveatContext     *[]byte
	DeletedAtUnixNano *int64
}

func (y *ydbDatastore) watch(
	ctx context.Context,
	afterRevision datastore.Revision,
	opts datastore.WatchOptions,
	updates chan *datastore.RevisionChanges,
	errs chan error,
) {
	defer close(updates)
	defer close(errs)

	afterTimestampRevision, ok := afterRevision.(revisions.TimestampRevision)
	if !ok {
		errs <- fmt.Errorf("expected timestamp revision, got  %T", afterRevision)
		return
	}

	tableToTopicName := map[string]string{
		tableRelationTuple:   y.config.tablePathPrefix + "/" + tableRelationTuple + "/" + changefeedSpicedbWatch,
		tableCaveat:          y.config.tablePathPrefix + "/" + tableCaveat + "/" + changefeedSpicedbWatch,
		tableNamespaceConfig: y.config.tablePathPrefix + "/" + tableNamespaceConfig + "/" + changefeedSpicedbWatch,
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

	var selectors topicoptions.ReadSelectors
	for _, topic := range topics {
		selectors = append(selectors,
			topicoptions.ReadSelector{
				Path:     topic,
				ReadFrom: afterTimestampRevision.Time(),
			},
		)
	}

	reader, err := y.driver.Topic().StartReader(
		uuid.New().String(),
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

	var (
		rErr    error
		msg     *topicreader.Message
		event   cdcEvent
		tracked = common.NewChanges(revisions.TimestampIDKeyFunc, opts.Content)
	)
	for msg, rErr = reader.ReadMessage(ctx); rErr != nil; msg, rErr = reader.ReadMessage(ctx) {
		if err := topicsugar.JSONUnmarshal(msg, &event); err != nil {
			sendError(fmt.Errorf("failed to unmarshal cdc event: %w", err))
			return
		}

		// todo
		// Resolved indicates that the specified revision is "complete"; no additional updates can come in before or at it.
		// Therefore, at this point, we issue tracked updates from before that time, and the checkpoint update.
		if false {
			rev := revisions.NewForTimestamp(1)
			changes := tracked.FilterAndRemoveRevisionChanges(revisions.TimestampIDKeyLessThanFunc, rev)
			for i := range changes {
				if !sendChange(&changes[i]) {
					return
				}
			}

			if opts.Content&datastore.WatchCheckpoints == datastore.WatchCheckpoints {
				if !sendChange(&datastore.RevisionChanges{
					Revision:     rev,
					IsCheckpoint: true,
				}) {
					return
				}
			}
			continue
		}

		switch topicToTableName[msg.Topic()] {
		case tableRelationTuple:
			if len(event.Key) != 7 {
				err := spiceerrors.MustBugf(
					"unexpected PK size. want 7, got %d (%q)",
					len(event.Key), strings.Join(event.Key, ","),
				)
				sendError(err)
				return
			}

			createdAtUnixNano, _ := strconv.Atoi(event.Key[6])
			createRev := revisions.NewForTimestamp(int64(createdAtUnixNano))

			tuple := &core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: event.Key[0],
					ObjectId:  event.Key[1],
					Relation:  event.Key[2],
				},
				Subject: &core.ObjectAndRelation{
					Namespace: event.Key[3],
					ObjectId:  event.Key[4],
					Relation:  event.Key[5],
				},
			}

			if event.NewImage != nil {
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

				if changes.DeletedAtUnixNano == nil {
					err := tracked.AddRelationshipChange(ctx, createRev, tuple, core.RelationTupleUpdate_TOUCH)
					if err != nil {
						sendError(err)
						return
					}
				} else {
					deleteRev := revisions.NewForTimestamp(*changes.DeletedAtUnixNano)
					err := tracked.AddRelationshipChange(ctx, deleteRev, tuple, core.RelationTupleUpdate_DELETE)
					if err != nil {
						sendError(err)
						return
					}
				}
			}

		case tableNamespaceConfig:
			if len(event.Key) != 2 {
				err := spiceerrors.MustBugf(
					"unexpected PK size. want 2, got %d (%q)",
					len(event.Key), strings.Join(event.Key, ","),
				)
				sendError(err)
				return
			}

			namespaceName := event.Key[0]
			createdAtUnixNano, _ := strconv.Atoi(event.Key[1])
			createRev := revisions.NewForTimestamp(int64(createdAtUnixNano))

			if event.NewImage != nil {
				var changes namespaceConfigImage
				if err := json.Unmarshal(event.NewImage, &changes); err != nil {
					sendError(fmt.Errorf("failed to unmarshal namespace new image: %w", err))
					return
				}

				var loaded core.NamespaceDefinition
				if err := loaded.UnmarshalVT(changes.SerializedConfig); err != nil {
					sendError(fmt.Errorf("failed to unmarshal namespace config: %w", err))
					return
				}

				if changes.DeletedAtUnixNano == nil {
					tracked.AddChangedDefinition(ctx, createRev, &loaded)
				} else {
					deleteRev := revisions.NewForTimestamp(*changes.DeletedAtUnixNano)
					tracked.AddDeletedNamespace(ctx, deleteRev, namespaceName)
				}
			}

		case tableCaveat:
			if len(event.Key) != 2 {
				err := spiceerrors.MustBugf(
					"unexpected PK size. want 2, got %d (%q)",
					len(event.Key), strings.Join(event.Key, ","),
				)
				sendError(err)
				return
			}

			caveatName := event.Key[0]
			createdAtUnixNano, _ := strconv.Atoi(event.Key[1])
			createRev := revisions.NewForTimestamp(int64(createdAtUnixNano))

			if event.NewImage != nil {
				var changes caveatImage
				if err := json.Unmarshal(event.NewImage, &changes); err != nil {
					sendError(fmt.Errorf("failed to unmarshal caveat new image: %w", err))
					return
				}

				var loaded core.CaveatDefinition
				if err := loaded.UnmarshalVT(changes.Definition); err != nil {
					sendError(fmt.Errorf("failed to unmarshal caveat config: %w", err))
					return
				}

				if changes.DeletedAtUnixNano == nil {
					tracked.AddChangedDefinition(ctx, createRev, &loaded)
				} else {
					deleteRev := revisions.NewForTimestamp(*changes.DeletedAtUnixNano)
					tracked.AddDeletedNamespace(ctx, deleteRev, caveatName)
				}
			}
		}

		if err := reader.Commit(ctx, msg); err != nil {
			sendError(fmt.Errorf("failed to commit offset: %w", err))
			return
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
