package planning

import (
	"fmt"
	"simple-db-go/types"
)

type NotQueryStatementError struct {
	sql types.SQL
}

func (e NotQueryStatementError) Error() string {
	return fmt.Sprintf("SELECT文でないSQLで QueryPlan を作成しようとしました. sql=%s", e.sql)
}

type NotUpdateStatementError struct {
	sql types.SQL
}

func (e NotUpdateStatementError) Error() string {
	return fmt.Sprintf("UPDATE文でないSQLで UpdatePlan を作成しようとしました. sql=%s", e.sql)
}
