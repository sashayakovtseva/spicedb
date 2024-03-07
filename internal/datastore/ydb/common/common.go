package common

import (
	"bytes"
	"net/url"
	"sync"
)

type DSN struct {
	OriginalDSN     string
	TablePathPrefix string
}

// ParseDSN is used to extract custom parameters from YDB DSN that are used to alter datastore behaviour.
// The following parameters are recognized:
//
//   - table_path_prefix: string. Will be added to all queries.
func ParseDSN(dsn string) DSN {
	parsedDSN := DSN{OriginalDSN: dsn}

	uri, err := url.Parse(dsn)
	if err != nil {
		// don't care about the error since ydb.Open will parse dsn again.
		return parsedDSN
	}

	params := uri.Query()
	parsedDSN.TablePathPrefix = params.Get("table_path_prefix")

	return parsedDSN
}

var bytesPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func AddTablePrefix(query string, tablePathPrefix string) string {
	if tablePathPrefix == "" {
		return query
	}

	buffer := bytesPool.Get().(*bytes.Buffer)
	defer func() {
		buffer.Reset()
		bytesPool.Put(buffer)
	}()

	buffer.WriteString("PRAGMA TablePathPrefix(\"")
	buffer.WriteString(tablePathPrefix)
	buffer.WriteString("\");\n\n")
	buffer.WriteString(query)

	return buffer.String()
}
