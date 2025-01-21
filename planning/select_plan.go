package planning

import (
	"simple-db-go/query"
	"simple-db-go/record"
	"simple-db-go/types"
)

// こうやって interface の実装を確認できるのか. 勉強として書いておく.
var _ query.Plan = (*SelectPlan)(nil)

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

func (p *SelectPlan) GetDistinctValues(fieldName types.FieldName) types.Int {
	_, err := p.predicate.EquatesWithConstant(fieldName)
	if err == nil {
		// Predicate において、引数の`fieldName`は何かしらの定数と等価比較されている.
		// つまり、Select の結果とりうる値は 1 つだけになるので 1 を返す.
		return 1
	}

	otherFieldName, err := p.predicate.EquatesWithFieldName(fieldName)
	if err == nil {
		// Predicate において、引数の`fieldName`は他のフィールドと等価比較されている.
		// よって、より少ない方のとりうる値に推定される
		// 教科書 10.2.2 を参照.
		return min(
			p.plan.GetDistinctValues(fieldName),
			p.plan.GetDistinctValues(otherFieldName),
		)
	} else {
		// それ以外の場合は、元の plan に委譲する.
		// つまり、predicate で参照されていない fieldName の場合が該当する.
		return p.plan.GetDistinctValues(fieldName)
	}
}

func (p *SelectPlan) GetSchema() *record.Schema {
	return p.plan.GetSchema()
}
