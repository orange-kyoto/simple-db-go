package data

import (
	"simple-db-go/query"
	"simple-db-go/types"
)

type InsertData struct {
	TableName  types.TableName
	FieldNames []types.FieldName
	Values     []query.Constant
}

func (*InsertData) SQLData() {}
