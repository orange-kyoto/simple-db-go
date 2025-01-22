package planning

import (
	"simple-db-go/query"
	"simple-db-go/record"
	"simple-db-go/types"
)

var _ query.Plan = (*ProductPlan)(nil)

type ProductPlan struct {
	plan1  query.Plan
	plan2  query.Plan
	schema *record.Schema
}

func NewProductPlan(plan1 query.Plan, plan2 query.Plan) query.Plan {
	schema := record.NewSchema()
	schema.AddAll(plan1.GetSchema())
	schema.AddAll(plan2.GetSchema())

	return &ProductPlan{plan1, plan2, schema}
}

func (p *ProductPlan) Open() query.Scan {
	return query.NewProductScan(p.plan1.Open(), p.plan2.Open())
}

// NOTE: ProductScan の実装を思い出すと、scan1 の Next() を呼ぶたびに、scan２は先頭に戻っていた.
func (p *ProductPlan) GetBlocksAccessed() types.Int {
	return p.plan1.GetBlocksAccessed() +
		(p.plan1.GetRecordsOutput() * p.plan2.GetBlocksAccessed())
}

func (p *ProductPlan) GetRecordsOutput() types.Int {
	return p.plan1.GetRecordsOutput() * p.plan2.GetRecordsOutput()
}

func (p *ProductPlan) GetDistinctValues(fieldName types.FieldName) types.Int {
	if p.plan1.GetSchema().HasField(fieldName) {
		return p.plan1.GetDistinctValues(fieldName)
	} else {
		return p.plan2.GetDistinctValues(fieldName)
	}
}

func (p *ProductPlan) GetSchema() *record.Schema {
	return p.schema
}
