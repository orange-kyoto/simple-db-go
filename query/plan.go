package query

import (
	"simple-db-go/record"
	"simple-db-go/types"
)

// Scan と似ているが、Scan は自身のデータにアクセスするのに対して、
// Plan はメタデータにアクセスする.
// Planning するためのメタデータを提供しつつ、対応する Scan を返すための`Open`メソッドを実装する.
type Plan interface {
	Open() Scan
	GetBlocksAccessed() types.Int
	GetRecordsOutput() types.Int
	GetDistinctValues(fieldName types.FieldName) types.Int
	// 各Planが出力するテーブルの schema を返す.
	GetSchema() *record.Schema
}
