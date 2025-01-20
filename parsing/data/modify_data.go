package data

import (
	"simple-db-go/query"
	"simple-db-go/types"
)

type ModifyData struct {
	TableName types.TableName
	FieldName types.FieldName
	NewValue  query.Expression
	Predicate *query.Predicate
}

func (*ModifyData) SQLData() {}
