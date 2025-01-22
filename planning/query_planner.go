package planning

import (
	"simple-db-go/parsing/data"
	"simple-db-go/query"
	"simple-db-go/transaction"
)

type QueryPlanner interface {
	CreatePlan(data *data.QueryData, transaction *transaction.Transaction) (query.Plan, error)
}
