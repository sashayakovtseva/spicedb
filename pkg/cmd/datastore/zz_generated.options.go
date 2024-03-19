// Code generated by github.com/ecordell/optgen. DO NOT EDIT.
package datastore

import (
	defaults "github.com/creasty/defaults"
	helpers "github.com/ecordell/optgen/helpers"
	"time"
)

type ConfigOption func(c *Config)

// NewConfigWithOptions creates a new Config with the passed in options set
func NewConfigWithOptions(opts ...ConfigOption) *Config {
	c := &Config{}
	for _, o := range opts {
		o(c)
	}
	return c
}

// NewConfigWithOptionsAndDefaults creates a new Config with the passed in options set starting from the defaults
func NewConfigWithOptionsAndDefaults(opts ...ConfigOption) *Config {
	c := &Config{}
	defaults.MustSet(c)
	for _, o := range opts {
		o(c)
	}
	return c
}

// ToOption returns a new ConfigOption that sets the values from the passed in Config
func (c *Config) ToOption() ConfigOption {
	return func(to *Config) {
		to.Engine = c.Engine
		to.URI = c.URI
		to.GCWindow = c.GCWindow
		to.LegacyFuzzing = c.LegacyFuzzing
		to.RevisionQuantization = c.RevisionQuantization
		to.MaxRevisionStalenessPercent = c.MaxRevisionStalenessPercent
		to.ReadConnPool = c.ReadConnPool
		to.WriteConnPool = c.WriteConnPool
		to.ReadOnly = c.ReadOnly
		to.EnableDatastoreMetrics = c.EnableDatastoreMetrics
		to.DisableStats = c.DisableStats
		to.BootstrapFiles = c.BootstrapFiles
		to.BootstrapFileContents = c.BootstrapFileContents
		to.BootstrapOverwrite = c.BootstrapOverwrite
		to.BootstrapTimeout = c.BootstrapTimeout
		to.RequestHedgingEnabled = c.RequestHedgingEnabled
		to.RequestHedgingInitialSlowValue = c.RequestHedgingInitialSlowValue
		to.RequestHedgingMaxRequests = c.RequestHedgingMaxRequests
		to.RequestHedgingQuantile = c.RequestHedgingQuantile
		to.FollowerReadDelay = c.FollowerReadDelay
		to.GCInterval = c.GCInterval
		to.GCMaxOperationTime = c.GCMaxOperationTime
		to.TablePrefix = c.TablePrefix
		to.MaxRetries = c.MaxRetries
		to.OverlapKey = c.OverlapKey
		to.OverlapStrategy = c.OverlapStrategy
		to.EnableConnectionBalancing = c.EnableConnectionBalancing
		to.ConnectRate = c.ConnectRate
		to.SpannerCredentialsFile = c.SpannerCredentialsFile
		to.SpannerEmulatorHost = c.SpannerEmulatorHost
		to.SpannerMinSessions = c.SpannerMinSessions
		to.SpannerMaxSessions = c.SpannerMaxSessions
		to.YDBCertificatePath = c.YDBCertificatePath
		to.YDBBulkLoadBatchSize = c.YDBBulkLoadBatchSize
		to.YDBEnableUniquenessCheck = c.YDBEnableUniquenessCheck
		to.WatchBufferLength = c.WatchBufferLength
		to.WatchBufferWriteTimeout = c.WatchBufferWriteTimeout
		to.MigrationPhase = c.MigrationPhase
	}
}

// DebugMap returns a map form of Config for debugging
func (c Config) DebugMap() map[string]any {
	debugMap := map[string]any{}
	debugMap["Engine"] = helpers.DebugValue(c.Engine, false)
	debugMap["URI"] = helpers.SensitiveDebugValue(c.URI)
	debugMap["GCWindow"] = helpers.DebugValue(c.GCWindow, false)
	debugMap["LegacyFuzzing"] = helpers.DebugValue(c.LegacyFuzzing, false)
	debugMap["RevisionQuantization"] = helpers.DebugValue(c.RevisionQuantization, false)
	debugMap["MaxRevisionStalenessPercent"] = helpers.DebugValue(c.MaxRevisionStalenessPercent, false)
	debugMap["ReadConnPool"] = helpers.DebugValue(c.ReadConnPool, false)
	debugMap["WriteConnPool"] = helpers.DebugValue(c.WriteConnPool, false)
	debugMap["ReadOnly"] = helpers.DebugValue(c.ReadOnly, false)
	debugMap["EnableDatastoreMetrics"] = helpers.DebugValue(c.EnableDatastoreMetrics, false)
	debugMap["DisableStats"] = helpers.DebugValue(c.DisableStats, false)
	debugMap["BootstrapFiles"] = helpers.DebugValue(c.BootstrapFiles, true)
	debugMap["BootstrapFileContents"] = helpers.DebugValue(c.BootstrapFileContents, false)
	debugMap["BootstrapOverwrite"] = helpers.DebugValue(c.BootstrapOverwrite, false)
	debugMap["BootstrapTimeout"] = helpers.DebugValue(c.BootstrapTimeout, false)
	debugMap["RequestHedgingEnabled"] = helpers.DebugValue(c.RequestHedgingEnabled, false)
	debugMap["RequestHedgingInitialSlowValue"] = helpers.DebugValue(c.RequestHedgingInitialSlowValue, false)
	debugMap["RequestHedgingMaxRequests"] = helpers.DebugValue(c.RequestHedgingMaxRequests, false)
	debugMap["RequestHedgingQuantile"] = helpers.DebugValue(c.RequestHedgingQuantile, false)
	debugMap["FollowerReadDelay"] = helpers.DebugValue(c.FollowerReadDelay, false)
	debugMap["GCInterval"] = helpers.DebugValue(c.GCInterval, false)
	debugMap["GCMaxOperationTime"] = helpers.DebugValue(c.GCMaxOperationTime, false)
	debugMap["TablePrefix"] = helpers.DebugValue(c.TablePrefix, false)
	debugMap["MaxRetries"] = helpers.DebugValue(c.MaxRetries, false)
	debugMap["OverlapKey"] = helpers.DebugValue(c.OverlapKey, false)
	debugMap["OverlapStrategy"] = helpers.DebugValue(c.OverlapStrategy, false)
	debugMap["EnableConnectionBalancing"] = helpers.DebugValue(c.EnableConnectionBalancing, false)
	debugMap["ConnectRate"] = helpers.DebugValue(c.ConnectRate, false)
	debugMap["SpannerCredentialsFile"] = helpers.DebugValue(c.SpannerCredentialsFile, false)
	debugMap["SpannerEmulatorHost"] = helpers.DebugValue(c.SpannerEmulatorHost, false)
	debugMap["SpannerMinSessions"] = helpers.DebugValue(c.SpannerMinSessions, false)
	debugMap["SpannerMaxSessions"] = helpers.DebugValue(c.SpannerMaxSessions, false)
	debugMap["YDBCertificatePath"] = helpers.DebugValue(c.YDBCertificatePath, false)
	debugMap["YDBBulkLoadBatchSize"] = helpers.DebugValue(c.YDBBulkLoadBatchSize, false)
	debugMap["YDBEnableUniquenessCheck"] = helpers.DebugValue(c.YDBEnableUniquenessCheck, false)
	debugMap["WatchBufferLength"] = helpers.DebugValue(c.WatchBufferLength, false)
	debugMap["WatchBufferWriteTimeout"] = helpers.DebugValue(c.WatchBufferWriteTimeout, false)
	debugMap["MigrationPhase"] = helpers.DebugValue(c.MigrationPhase, false)
	return debugMap
}

// ConfigWithOptions configures an existing Config with the passed in options set
func ConfigWithOptions(c *Config, opts ...ConfigOption) *Config {
	for _, o := range opts {
		o(c)
	}
	return c
}

// WithOptions configures the receiver Config with the passed in options set
func (c *Config) WithOptions(opts ...ConfigOption) *Config {
	for _, o := range opts {
		o(c)
	}
	return c
}

// WithEngine returns an option that can set Engine on a Config
func WithEngine(engine string) ConfigOption {
	return func(c *Config) {
		c.Engine = engine
	}
}

// WithURI returns an option that can set URI on a Config
func WithURI(uRI string) ConfigOption {
	return func(c *Config) {
		c.URI = uRI
	}
}

// WithGCWindow returns an option that can set GCWindow on a Config
func WithGCWindow(gCWindow time.Duration) ConfigOption {
	return func(c *Config) {
		c.GCWindow = gCWindow
	}
}

// WithLegacyFuzzing returns an option that can set LegacyFuzzing on a Config
func WithLegacyFuzzing(legacyFuzzing time.Duration) ConfigOption {
	return func(c *Config) {
		c.LegacyFuzzing = legacyFuzzing
	}
}

// WithRevisionQuantization returns an option that can set RevisionQuantization on a Config
func WithRevisionQuantization(revisionQuantization time.Duration) ConfigOption {
	return func(c *Config) {
		c.RevisionQuantization = revisionQuantization
	}
}

// WithMaxRevisionStalenessPercent returns an option that can set MaxRevisionStalenessPercent on a Config
func WithMaxRevisionStalenessPercent(maxRevisionStalenessPercent float64) ConfigOption {
	return func(c *Config) {
		c.MaxRevisionStalenessPercent = maxRevisionStalenessPercent
	}
}

// WithReadConnPool returns an option that can set ReadConnPool on a Config
func WithReadConnPool(readConnPool ConnPoolConfig) ConfigOption {
	return func(c *Config) {
		c.ReadConnPool = readConnPool
	}
}

// WithWriteConnPool returns an option that can set WriteConnPool on a Config
func WithWriteConnPool(writeConnPool ConnPoolConfig) ConfigOption {
	return func(c *Config) {
		c.WriteConnPool = writeConnPool
	}
}

// WithReadOnly returns an option that can set ReadOnly on a Config
func WithReadOnly(readOnly bool) ConfigOption {
	return func(c *Config) {
		c.ReadOnly = readOnly
	}
}

// WithEnableDatastoreMetrics returns an option that can set EnableDatastoreMetrics on a Config
func WithEnableDatastoreMetrics(enableDatastoreMetrics bool) ConfigOption {
	return func(c *Config) {
		c.EnableDatastoreMetrics = enableDatastoreMetrics
	}
}

// WithDisableStats returns an option that can set DisableStats on a Config
func WithDisableStats(disableStats bool) ConfigOption {
	return func(c *Config) {
		c.DisableStats = disableStats
	}
}

// WithBootstrapFiles returns an option that can append BootstrapFiless to Config.BootstrapFiles
func WithBootstrapFiles(bootstrapFiles string) ConfigOption {
	return func(c *Config) {
		c.BootstrapFiles = append(c.BootstrapFiles, bootstrapFiles)
	}
}

// SetBootstrapFiles returns an option that can set BootstrapFiles on a Config
func SetBootstrapFiles(bootstrapFiles []string) ConfigOption {
	return func(c *Config) {
		c.BootstrapFiles = bootstrapFiles
	}
}

// WithBootstrapFileContents returns an option that can append BootstrapFileContentss to Config.BootstrapFileContents
func WithBootstrapFileContents(key string, value []byte) ConfigOption {
	return func(c *Config) {
		c.BootstrapFileContents[key] = value
	}
}

// SetBootstrapFileContents returns an option that can set BootstrapFileContents on a Config
func SetBootstrapFileContents(bootstrapFileContents map[string][]byte) ConfigOption {
	return func(c *Config) {
		c.BootstrapFileContents = bootstrapFileContents
	}
}

// WithBootstrapOverwrite returns an option that can set BootstrapOverwrite on a Config
func WithBootstrapOverwrite(bootstrapOverwrite bool) ConfigOption {
	return func(c *Config) {
		c.BootstrapOverwrite = bootstrapOverwrite
	}
}

// WithBootstrapTimeout returns an option that can set BootstrapTimeout on a Config
func WithBootstrapTimeout(bootstrapTimeout time.Duration) ConfigOption {
	return func(c *Config) {
		c.BootstrapTimeout = bootstrapTimeout
	}
}

// WithRequestHedgingEnabled returns an option that can set RequestHedgingEnabled on a Config
func WithRequestHedgingEnabled(requestHedgingEnabled bool) ConfigOption {
	return func(c *Config) {
		c.RequestHedgingEnabled = requestHedgingEnabled
	}
}

// WithRequestHedgingInitialSlowValue returns an option that can set RequestHedgingInitialSlowValue on a Config
func WithRequestHedgingInitialSlowValue(requestHedgingInitialSlowValue time.Duration) ConfigOption {
	return func(c *Config) {
		c.RequestHedgingInitialSlowValue = requestHedgingInitialSlowValue
	}
}

// WithRequestHedgingMaxRequests returns an option that can set RequestHedgingMaxRequests on a Config
func WithRequestHedgingMaxRequests(requestHedgingMaxRequests uint64) ConfigOption {
	return func(c *Config) {
		c.RequestHedgingMaxRequests = requestHedgingMaxRequests
	}
}

// WithRequestHedgingQuantile returns an option that can set RequestHedgingQuantile on a Config
func WithRequestHedgingQuantile(requestHedgingQuantile float64) ConfigOption {
	return func(c *Config) {
		c.RequestHedgingQuantile = requestHedgingQuantile
	}
}

// WithFollowerReadDelay returns an option that can set FollowerReadDelay on a Config
func WithFollowerReadDelay(followerReadDelay time.Duration) ConfigOption {
	return func(c *Config) {
		c.FollowerReadDelay = followerReadDelay
	}
}

// WithGCInterval returns an option that can set GCInterval on a Config
func WithGCInterval(gCInterval time.Duration) ConfigOption {
	return func(c *Config) {
		c.GCInterval = gCInterval
	}
}

// WithGCMaxOperationTime returns an option that can set GCMaxOperationTime on a Config
func WithGCMaxOperationTime(gCMaxOperationTime time.Duration) ConfigOption {
	return func(c *Config) {
		c.GCMaxOperationTime = gCMaxOperationTime
	}
}

// WithTablePrefix returns an option that can set TablePrefix on a Config
func WithTablePrefix(tablePrefix string) ConfigOption {
	return func(c *Config) {
		c.TablePrefix = tablePrefix
	}
}

// WithMaxRetries returns an option that can set MaxRetries on a Config
func WithMaxRetries(maxRetries int) ConfigOption {
	return func(c *Config) {
		c.MaxRetries = maxRetries
	}
}

// WithOverlapKey returns an option that can set OverlapKey on a Config
func WithOverlapKey(overlapKey string) ConfigOption {
	return func(c *Config) {
		c.OverlapKey = overlapKey
	}
}

// WithOverlapStrategy returns an option that can set OverlapStrategy on a Config
func WithOverlapStrategy(overlapStrategy string) ConfigOption {
	return func(c *Config) {
		c.OverlapStrategy = overlapStrategy
	}
}

// WithEnableConnectionBalancing returns an option that can set EnableConnectionBalancing on a Config
func WithEnableConnectionBalancing(enableConnectionBalancing bool) ConfigOption {
	return func(c *Config) {
		c.EnableConnectionBalancing = enableConnectionBalancing
	}
}

// WithConnectRate returns an option that can set ConnectRate on a Config
func WithConnectRate(connectRate time.Duration) ConfigOption {
	return func(c *Config) {
		c.ConnectRate = connectRate
	}
}

// WithSpannerCredentialsFile returns an option that can set SpannerCredentialsFile on a Config
func WithSpannerCredentialsFile(spannerCredentialsFile string) ConfigOption {
	return func(c *Config) {
		c.SpannerCredentialsFile = spannerCredentialsFile
	}
}

// WithSpannerEmulatorHost returns an option that can set SpannerEmulatorHost on a Config
func WithSpannerEmulatorHost(spannerEmulatorHost string) ConfigOption {
	return func(c *Config) {
		c.SpannerEmulatorHost = spannerEmulatorHost
	}
}

// WithSpannerMinSessions returns an option that can set SpannerMinSessions on a Config
func WithSpannerMinSessions(spannerMinSessions uint64) ConfigOption {
	return func(c *Config) {
		c.SpannerMinSessions = spannerMinSessions
	}
}

// WithSpannerMaxSessions returns an option that can set SpannerMaxSessions on a Config
func WithSpannerMaxSessions(spannerMaxSessions uint64) ConfigOption {
	return func(c *Config) {
		c.SpannerMaxSessions = spannerMaxSessions
	}
}

// WithYDBCertificatePath returns an option that can set YDBCertificatePath on a Config
func WithYDBCertificatePath(yDBCertificatePath string) ConfigOption {
	return func(c *Config) {
		c.YDBCertificatePath = yDBCertificatePath
	}
}

// WithYDBBulkLoadBatchSize returns an option that can set YDBBulkLoadBatchSize on a Config
func WithYDBBulkLoadBatchSize(yDBBulkLoadBatchSize int) ConfigOption {
	return func(c *Config) {
		c.YDBBulkLoadBatchSize = yDBBulkLoadBatchSize
	}
}

// WithYDBEnableUniquenessCheck returns an option that can set YDBEnableUniquenessCheck on a Config
func WithYDBEnableUniquenessCheck(yDBEnableUniquenessCheck bool) ConfigOption {
	return func(c *Config) {
		c.YDBEnableUniquenessCheck = yDBEnableUniquenessCheck
	}
}

// WithWatchBufferLength returns an option that can set WatchBufferLength on a Config
func WithWatchBufferLength(watchBufferLength uint16) ConfigOption {
	return func(c *Config) {
		c.WatchBufferLength = watchBufferLength
	}
}

// WithWatchBufferWriteTimeout returns an option that can set WatchBufferWriteTimeout on a Config
func WithWatchBufferWriteTimeout(watchBufferWriteTimeout time.Duration) ConfigOption {
	return func(c *Config) {
		c.WatchBufferWriteTimeout = watchBufferWriteTimeout
	}
}

// WithMigrationPhase returns an option that can set MigrationPhase on a Config
func WithMigrationPhase(migrationPhase string) ConfigOption {
	return func(c *Config) {
		c.MigrationPhase = migrationPhase
	}
}
