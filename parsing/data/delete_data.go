package data

import (
	"simple-db-go/query"
	"simple-db-go/types"
)

type DeleteData struct {
	TableName types.TableName
	Predicate *query.Predicate
}

func (*DeleteData) SQLData() {}
