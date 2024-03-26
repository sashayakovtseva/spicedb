package ydb

import (
	"time"
)

type ydbConfig struct {
	tablePathPrefix string
	certificatePath string

	watchBufferLength       uint16
	watchBufferWriteTimeout time.Duration

	followerReadDelay           time.Duration
	revisionQuantization        time.Duration
	maxRevisionStalenessPercent float64

	gcWindow           time.Duration
	gcInterval         time.Duration
	gcMaxOperationTime time.Duration

	sessionCountLimit    int
	sessionIdleThreshold time.Duration

	bulkLoadBatchSize int

	enableGC              bool
	enablePrometheusStats bool
	enableUniquenessCheck bool
}

var defaultConfig = ydbConfig{
	tablePathPrefix:             "",
	certificatePath:             "",
	watchBufferLength:           128,
	watchBufferWriteTimeout:     time.Second,
	followerReadDelay:           0,
	revisionQuantization:        5 * time.Second,
	maxRevisionStalenessPercent: 0.1,
	gcWindow:                    24 * time.Hour,
	gcInterval:                  3 * time.Minute,
	gcMaxOperationTime:          time.Minute,
	sessionCountLimit:           50,
	sessionIdleThreshold:        5 * time.Minute,
	bulkLoadBatchSize:           1000,
	enableGC:                    true,
	enablePrometheusStats:       false,
	enableUniquenessCheck:       true,
}

// Option provides the facility to configure how clients within the YDB
// datastore interact with the running YDB database.
type Option func(*ydbConfig)

func generateConfig(options []Option) *ydbConfig {
	computed := defaultConfig
	for _, option := range options {
		option(&computed)
	}

	return &computed
}

// WithTablePathPrefix sets table prefix that will be implicitly added to all YDB queries.
// See https://ydb.tech/docs/en/yql/reference/syntax/pragma#table-path-prefix for details.
//
// Default is empty.
// Non-empty DSN parameter takes precedence over this option.
func WithTablePathPrefix(prefix string) Option {
	return func(o *ydbConfig) { o.tablePathPrefix = prefix }
}

// WithCertificatePath sets a path to the certificate to use when connecting to YDB with grpcs protocol.
// Default is empty.
func WithCertificatePath(path string) Option {
	return func(o *ydbConfig) { o.certificatePath = path }
}

// WithSessionCountLimit sets the maximum size of internal session pool in table.Client.
//
// This value defaults to 50.
func WithSessionCountLimit(in int) Option {
	return func(o *ydbConfig) { o.sessionCountLimit = in }
}

// WithSessionIdleThreshold defines idle session lifetime threshold.
//
// This value defaults to 5 minutes.
func WithSessionIdleThreshold(in time.Duration) Option {
	return func(o *ydbConfig) { o.sessionIdleThreshold = in }
}

// GCWindow is the maximum age of a passed revision that will be considered
// valid.
//
// This value defaults to 24 hours.
func GCWindow(window time.Duration) Option {
	return func(o *ydbConfig) { o.gcWindow = window }
}

// GCInterval is the interval at which garbage collection will occur.
//
// This value defaults to 3 minutes.
func GCInterval(interval time.Duration) Option {
	return func(o *ydbConfig) { o.gcInterval = interval }
}

// WithEnableGC indicates whether garbage collection is enabled.
//
// GC is enabled by default.
func WithEnableGC(isGCEnabled bool) Option {
	return func(o *ydbConfig) { o.enableGC = isGCEnabled }
}

// GCMaxOperationTime is the maximum operation time of a garbage collection pass before it times out.
//
// This value defaults to 1 minute.
func GCMaxOperationTime(time time.Duration) Option {
	return func(o *ydbConfig) { o.gcMaxOperationTime = time }
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

// FollowerReadDelay is the time delay to apply to enable historical reads.
//
// This value defaults to 0 seconds.
func FollowerReadDelay(delay time.Duration) Option {
	return func(o *ydbConfig) { o.followerReadDelay = delay }
}

// BulkLoadBatchSize is the number of rows BulkLoad will process in a single batch.
//
// This value defaults to 1000.
func BulkLoadBatchSize(limit int) Option {
	return func(o *ydbConfig) { o.bulkLoadBatchSize = limit }
}

// WatchBufferLength is the number of entries that can be stored in the watch
// buffer while awaiting read by the client.
//
// This value defaults to 128.
func WatchBufferLength(watchBufferLength uint16) Option {
	return func(o *ydbConfig) { o.watchBufferLength = watchBufferLength }
}

// WatchBufferWriteTimeout is the maximum timeout for writing to the watch buffer,
// after which the caller to the watch will be disconnected.
func WatchBufferWriteTimeout(watchBufferWriteTimeout time.Duration) Option {
	return func(o *ydbConfig) { o.watchBufferWriteTimeout = watchBufferWriteTimeout }
}

// WithEnablePrometheusStats marks whether Prometheus metrics provided by the Postgres
// clients being used by the datastore are enabled.
//
// Prometheus metrics are disabled by default.
func WithEnablePrometheusStats(v bool) Option {
	return func(o *ydbConfig) { o.enablePrometheusStats = v }
}

// WithEnableUniquenessCheck marks whether relation tuples will be checked against
// unique index during CREATE operation. YDB doesn't support unique secondary indexes,
// and since this check is quite expensive one may turn it off.
func WithEnableUniquenessCheck(v bool) Option {
	return func(o *ydbConfig) { o.enableUniquenessCheck = v }
}
