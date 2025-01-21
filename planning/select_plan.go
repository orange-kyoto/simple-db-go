package planning

import (
	"simple-db-go/query"
	"simple-db-go/record"
	"simple-db-go/types"
)

type SelectPlan struct {
	plan      query.Plan
	predicate *query.Predicate
}

func NewSelectPlan(plan query.Plan, predicate *query.Predicate) *SelectPlan {
	return &SelectPlan{plan: plan, predicate: predicate}
}

func (p *SelectPlan) Open() query.Scan {
	scan := p.plan.Open()
	return query.NewSelectScan(scan, p.predicate)
}

// 内部の scan の Next() を結局呼んでいるので、ブロックアクセス数は変わらない.
func (p *SelectPlan) GetBlocksAccessed() types.Int {
	return p.plan.GetBlocksAccessed()
}

// Predicate によってどの程度レコード数が削減されるのか？が考慮すべきことになる.
// そのために、predicate.ReductionFactor() を呼び出す.
// 具体的には、predicate で参照されるフィールド（列）がとりうる値の数を見て、どの程度フィルタリングされるか割り算して計算する.
// SimpleDB では等価比較(`=`)しかサポートしていないので、シンプルな計算になる.
func (p *SelectPlan) GetRecordsOutput() types.Int {
	return p.plan.GetRecordsOutput() / p.predicate.GetReductionFactor(p.plan)
}

func (p *SelectPlan) GetSchema() *record.Schema {
	return p.plan.GetSchema()
}
