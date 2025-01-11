package query

import (
	"simple-db-go/record"
	"simple-db-go/types"
)

type UpdateScan interface {
	Scan

	SetInt(fieldName types.FieldName, value types.Int)
	SetString(fieldName types.FieldName, value string)
	SetValue(fieldName types.FieldName, value Constant)
	Insert()
	Delete()
	GetCurrentRecordID() record.RecordID
	MoveToRecordID(recordID record.RecordID)
}
