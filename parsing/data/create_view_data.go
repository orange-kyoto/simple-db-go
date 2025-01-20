package data

import "simple-db-go/types"

type CreateViewData struct {
	TableName types.TableName
	QueryData *QueryData
}

func (*CreateViewData) SQLData() {}
