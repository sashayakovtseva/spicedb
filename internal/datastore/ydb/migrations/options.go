package migrations

type ydbConfig struct {
	tablePathPrefix string
	certificatePath string
}

var defaultConfig = ydbConfig{}

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
