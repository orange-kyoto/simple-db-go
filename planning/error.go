package planning

import (
	"fmt"
)

type NotQueryStatementError struct {
	sql string
}

func (e NotQueryStatementError) Error() string {
	return fmt.Sprintf("SELECT文でないSQLで QueryPlan を作成しようとしました. sql=%s", e.sql)
}

type NotUpdateStatementError struct {
	sql string
}

func (e NotUpdateStatementError) Error() string {
	return fmt.Sprintf("UPDATE文でないSQLで UpdatePlan を作成しようとしました. sql=%s", e.sql)
}
