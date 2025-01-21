package data

import (
	"simple-db-go/record"
	"simple-db-go/types"
)

type CreateTableData struct {
	TableName types.TableName
	Schema    *record.Schema
}

func (*CreateTableData) SQLData() {}
