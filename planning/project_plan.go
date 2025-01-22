package planning

import (
	"simple-db-go/query"
	"simple-db-go/record"
	"simple-db-go/types"
)

var _ query.Plan = (*ProjectPlan)(nil)

type ProjectPlan struct {
	plan   query.Plan
	schema *record.Schema
}

func NewProjectPlan(plan query.Plan, fieldNames []types.FieldName) (*ProjectPlan, error) {
	schema := record.NewSchema()
	for _, fieldName := range fieldNames {
		err := schema.Add(fieldName, plan.GetSchema())
		if err != nil {
			return nil, err
		}
	}

	return &ProjectPlan{plan, schema}, nil
}

func (p *ProjectPlan) Open() query.Scan {
	scan := p.plan.Open()
	return query.NewProjectScan(scan, p.schema.Fields())
}

// 射影するだけなので、ブロックアクセス数は変わらない. 行指向だし.
func (p *ProjectPlan) GetBlocksAccessed() types.Int {
	return p.plan.GetBlocksAccessed()
}

// 射影するだけなので、レコード数も変わらない.
func (p *ProjectPlan) GetRecordsOutput() types.Int {
	return p.plan.GetRecordsOutput()
}

// 同様に、射影するだけなのでとりうる値も変わらない.
func (p *ProjectPlan) GetDistinctValues(fieldName types.FieldName) types.Int {
	return p.plan.GetDistinctValues(fieldName)
}

// projection した後の schema を返すことに注意.
func (p *ProjectPlan) GetSchema() *record.Schema {
	return p.schema
}
