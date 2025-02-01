package server

import "fmt"

type InvalidDBNameError struct {
	dbName string
}

func (e InvalidDBNameError) Error() string {
	return fmt.Sprintf("無効なDB名です: %s", e.dbName)
}

type NotTransactionCommandError struct {
	sql string
}

func (e NotTransactionCommandError) Error() string {
	return fmt.Sprintf("COMMIT, ROLLBACK ではありません: %s", e.sql)
}

type HandleQueryError struct {
	createQueryPlanError    error
	executeUpdateError      error
	transactionCommandError error
}

func (e HandleQueryError) Error() string {
	return fmt.Sprintf("クエリの処理に失敗しました: %v, %v, %v", e.createQueryPlanError, e.executeUpdateError, e.transactionCommandError)
}
