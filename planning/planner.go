package planning

import (
	"simple-db-go/parsing"
	"simple-db-go/parsing/data"
	"simple-db-go/query"
	"simple-db-go/transaction"
	"simple-db-go/types"
)

type Planner struct {
	queryPlanner  QueryPlanner
	updatePlanner UpdatePlanner
}

func NewPlanner(queryPlanner QueryPlanner, updatePlanner UpdatePlanner) *Planner {
	return &Planner{queryPlanner, updatePlanner}
}

func (p *Planner) CreateQueryPlan(sql types.SQL, transaction *transaction.Transaction) (query.Plan, error) {
	parser := parsing.NewParser()
	sqlData, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}

	if sqlData, ok := sqlData.(*data.QueryData); ok {
		return p.queryPlanner.CreatePlan(sqlData, transaction)
	} else {
		return nil, NotQueryStatementError{sql}
	}
}

func (p *Planner) ExecuteUpdate(sql types.SQL, transaction *transaction.Transaction) (types.Int, error) {
	parser := parsing.NewParser()
	sqlData, err := parser.Parse(sql)
	if err != nil {
		return 0, err
	}

	switch sqlData := sqlData.(type) {
	case *data.InsertData:
		return p.updatePlanner.ExecuteInsert(sqlData, transaction)
	case *data.DeleteData:
		return p.updatePlanner.ExecuteDelete(sqlData, transaction)
	case *data.ModifyData:
		return p.updatePlanner.ExecuteModify(sqlData, transaction)
	case *data.CreateTableData:
		return p.updatePlanner.ExecuteCreateTable(sqlData, transaction), nil
	case *data.CreateViewData:
		return p.updatePlanner.ExecuteCreateView(sqlData, transaction), nil
	case *data.CreateIndexData:
		return p.updatePlanner.ExecuteCreateIndex(sqlData, transaction), nil
	default:
		return 0, NotUpdateStatementError{sql}
	}
}
