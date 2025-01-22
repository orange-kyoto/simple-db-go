package planning

import (
	"simple-db-go/metadata"
	"simple-db-go/parsing"
	"simple-db-go/parsing/data"
	"simple-db-go/query"
	"simple-db-go/transaction"
)

var _ QueryPlanner = (*BetterQueryPlanner)(nil)

type BetterQueryPlanner struct {
	metadataManager *metadata.MetadataManager
}

func NewBetterQueryPlanner(metadataManager *metadata.MetadataManager) *BetterQueryPlanner {
	return &BetterQueryPlanner{metadataManager}
}

func (p *BetterQueryPlanner) CreatePlan(queryData *data.QueryData, transaction *transaction.Transaction) (query.Plan, error) {
	// Step1: FROM 句で指定されるテーブル、ビューのプランを作る.
	plans := make([]query.Plan, 0, len(queryData.Queryables))
	for _, queryable := range queryData.Queryables {
		viewDef, err := p.metadataManager.GetViewDef(queryable.ToViewName(), transaction)
		if err == nil { // queryable is view.
			parser := parsing.NewParser()
			viewData, err := parser.Parse(viewDef.ToString())
			if err != nil {
				return nil, err
			}
			// NOTE: ビューの定義は SELECT 文だけ許可するようパースしているので、QueryData と強制してOK.
			viewPlan, err := p.CreatePlan(viewData.(*data.QueryData), transaction)
			if err != nil {
				return nil, err
			}
			plans = append(plans, viewPlan)
		} else { // queryable is table.
			newPlan, err := NewTablePlan(transaction, queryable.ToTableName(), p.metadataManager)
			if err != nil {
				return nil, err
			}
			plans = append(plans, newPlan)
		}
	}

	// 注意：ここだけ BasicQueryPlanner と異なる！
	// Step2: Product Plan を作成する.
	plan := plans[0]
	for _, newPlan := range plans[1:] {
		p1 := NewProductPlan(plan, newPlan)
		p2 := NewProductPlan(newPlan, plan)

		// ブロックアクセス数だけで比較。まだマシかもしれない、というアルゴリズム.
		// 大事なのは、こんな感じでメタデータを使ってコストの低いプランを判断することができる、ということ.
		if p1.GetBlocksAccessed() < p2.GetBlocksAccessed() {
			plan = p1
		} else {
			plan = p2
		}
	}

	// Step3: WHERE 句で指定される条件を適用する.
	plan = NewSelectPlan(plan, queryData.Predicate)

	// Step4: Projection する.
	result, err := NewProjectPlan(plan, queryData.FieldNames)
	if err != nil {
		return nil, err
	}

	return result, nil
}
