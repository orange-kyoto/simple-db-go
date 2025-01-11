package query

import "simple-db-go/types"

type Scan interface {
	BeforeFirst()
	Next() bool
	GetInt(fieldName types.FieldName) types.Int
	GetString(fieldName types.FieldName) string
	GetValue(fieldName types.FieldName) Constant
	HasField(fieldName types.FieldName) bool
	Close()
}
