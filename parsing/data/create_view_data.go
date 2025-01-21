package data

import "simple-db-go/types"

type CreateViewData struct {
	ViewName  types.ViewName
	QueryData *QueryData
}

func (*CreateViewData) SQLData() {}
