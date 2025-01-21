package query

import (
	"simple-db-go/record"
	"simple-db-go/types"
)

type UpdateScan interface {
	Scan

	// 存在しないフィールドを指定されたらエラーを返す.
	SetInt(fieldName types.FieldName, value types.Int) error
	// 存在しないフィールドを指定されたらエラーを返す.
	SetString(fieldName types.FieldName, value string) error
	// 存在しないフィールドを指定されたらエラーを返す.
	SetValue(fieldName types.FieldName, value Constant) error

	Insert()
	Delete()
	GetCurrentRecordID() record.RecordID
	MoveToRecordID(recordID record.RecordID)
}
