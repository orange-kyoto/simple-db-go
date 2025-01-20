package data

import (
	"simple-db-go/record"
	"simple-db-go/types"
)

type InsertData struct {
	TableName  types.TableName
	FieldNames []types.FieldName
	Values     []record.Constant
}

func (*InsertData) SQLData() {}
