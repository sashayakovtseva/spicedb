package ydb

import (
	"time"
)

type ydbConfig struct {
	tablePathPrefix string

	watchBufferLength       uint16
	watchBufferWriteTimeout time.Duration

	followerReadDelay           time.Duration
	revisionQuantization        time.Duration
	maxRevisionStalenessPercent float64

	gcWindow           time.Duration
	gcInterval         time.Duration
	gcMaxOperationTime time.Duration

	maxRetries uint8

	gcEnabled             bool
	enablePrometheusStats bool
}

var defaultConfig = ydbConfig{
	tablePathPrefix:             "",
	watchBufferLength:           0,
	watchBufferWriteTimeout:     0,
	followerReadDelay:           5 * time.Second,
	revisionQuantization:        5 * time.Second,
	maxRevisionStalenessPercent: 0.1,
	gcWindow:                    24 * time.Hour,
	gcInterval:                  0,
	gcMaxOperationTime:          0,
	maxRetries:                  0,
	gcEnabled:                   false,
	enablePrometheusStats:       false,
}

// Option provides the facility to configure how clients within the YDB
// datastore interact with the running YDB database.
type Option func(*ydbConfig)

func generateConfig(options []Option) ydbConfig {
	computed := defaultConfig
	for _, option := range options {
		option(&computed)
	}

	return computed
}

// WithTablePathPrefix sets table prefix that will be implicitly added to all YDB queries.
// See https://ydb.tech/docs/en/yql/reference/syntax/pragma#table-path-prefix for details.
//
// Default is empty.
// Non-empty DSN parameter takes precedence over this option.
func WithTablePathPrefix(prefix string) Option {
	return func(o *ydbConfig) { o.tablePathPrefix = prefix }
}

// GCWindow is the maximum age of a passed revision that will be considered
// valid.
//
// This value defaults to 24 hours.
func GCWindow(window time.Duration) Option {
	return func(o *ydbConfig) { o.gcWindow = window }
}

// RevisionQuantization is the time bucket size to which advertised revisions
// will be rounded.
//
// This value defaults to 5 seconds.
func RevisionQuantization(bucketSize time.Duration) Option {
	return func(o *ydbConfig) { o.revisionQuantization = bucketSize }
}

// MaxRevisionStalenessPercent is the amount of time, expressed as a percentage of
// the revision quantization window, that a previously computed rounded revision
// can still be advertised after the next rounded revision would otherwise be ready.
//
// This value defaults to 0.1 (10%).
func MaxRevisionStalenessPercent(stalenessPercent float64) Option {
	return func(o *ydbConfig) { o.maxRevisionStalenessPercent = stalenessPercent }
}

// FollowerReadDelay is the time delay to apply to enable historial reads.
//
// This value defaults to 5 seconds.
func FollowerReadDelay(delay time.Duration) Option {
	return func(o *ydbConfig) { o.followerReadDelay = delay }
}
