package ydb

import "github.com/authzed/spicedb/pkg/datastore"

func init() {
	datastore.Engines = append(datastore.Engines, Engine)
}

const Engine = "ydb"
