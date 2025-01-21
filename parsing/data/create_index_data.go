package data

import "simple-db-go/types"

type CreateIndexData struct {
	IndexName types.IndexName
	TableName types.TableName
	FieldName types.FieldName
}

func (*CreateIndexData) SQLData() {}
