package planning

import (
	"simple-db-go/metadata"
	"simple-db-go/parsing"
	"simple-db-go/parsing/data"
	"simple-db-go/query"
	"simple-db-go/transaction"
)

var _ QueryPlanner = (*BasicQueryPlanner)(nil)

type BasicQueryPlanner struct {
	metadataManager *metadata.MetadataManager
}

func NewBasicQueryPlanner(metadataManager *metadata.MetadataManager) *BasicQueryPlanner {
	return &BasicQueryPlanner{metadataManager}
}

func (p *BasicQueryPlanner) CreatePlan(queryData *data.QueryData, transaction *transaction.Transaction) (query.Plan, error) {
	// Step1: FROM 句で指定されるテーブル、ビューのプランを作る.
	plans := make([]query.Plan, 0, len(queryData.Queryables))
	for _, queryable := range queryData.Queryables {
		viewDef, err := p.metadataManager.GetViewDef(queryable.ToViewName(), transaction)
		if err == nil { // queryable is view.
			parser := parsing.NewParser()
			viewData, err := parser.Parse(string(viewDef))
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

	// Step2: Product Plan を作成する.
	plan := plans[0]
	for _, newPlan := range plans[1:] {
		plan = NewProductPlan(plan, newPlan)
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
