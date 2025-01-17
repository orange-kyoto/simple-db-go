package query

import (
	"simple-db-go/record"
	"simple-db-go/types"
)

type Scan interface {
	BeforeFirst()

	Next() bool

	// 存在しないフィールドを指定されたらエラーを返す.
	GetInt(fieldName types.FieldName) (types.Int, error)

	// 存在しないフィールドを指定されたらエラーを返す.
	GetString(fieldName types.FieldName) (string, error)

	// 存在しないフィールドを指定されたらエラーを返す.
	GetValue(fieldName types.FieldName) (record.Constant, error)

	HasField(fieldName types.FieldName) bool

	Close()
}
