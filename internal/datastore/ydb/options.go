package ydb

import (
	"time"
)

type ydbConfig struct {
	tablePathPrefix string

	watchBufferLength       uint16
	watchBufferWriteTimeout time.Duration

	revisionQuantization        time.Duration
	maxRevisionStalenessPercent float64

	gcWindow           time.Duration
	gcInterval         time.Duration
	gcMaxOperationTime time.Duration

	maxRetries uint8

	gcEnabled             bool
	enablePrometheusStats bool
}

// Option provides the facility to configure how clients within the YDB
// datastore interact with the running YDB database.
type Option func(*ydbConfig)

func generateConfig(options []Option) ydbConfig {
	var computed ydbConfig
	for _, option := range options {
		option(&computed)
	}

	return computed
}

// WithTablePathPrefix sets table prefix that will be implicitly added to all YDB queries.
// See https://ydb.tech/docs/en/yql/reference/syntax/pragma#table-path-prefix for details.
//
// Default is empty.
// DSN parameter  takes precedence over this option.
func WithTablePathPrefix(prefix string) Option {
	return func(o *ydbConfig) { o.tablePathPrefix = prefix }
}
