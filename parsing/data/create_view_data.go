package data

import "simple-db-go/types"

type CreateViewData struct {
	ViewName  types.ViewName
	QueryData *QueryData
}

func (*CreateViewData) SQLData() {}

func (c *CreateViewData) GetViewDef() types.ViewDef {
	return types.ViewDef(c.QueryData.ToString())
}
