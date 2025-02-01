package server

import (
	"simple-db-go/parsing"
	"simple-db-go/parsing/data"
	"simple-db-go/query"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func doQuery(handler *SimpleDBSQLHandler, sql string) (*mysql.Result, error) {
	queryPlan, err := handler.planner.CreateQueryPlan(sql, handler.transaction)
	if err != nil {
		return nil, err
	}

	scan := queryPlan.Open()
	defer scan.Close()

	resultSet, err := buildResultsetFrom(scan)
	if err != nil {
		return nil, err
	}
	result := mysql.NewResult(resultSet)
	result.Status = mysql.SERVER_STATUS_IN_TRANS // AUTOCOMMIT=off がデフォルトの想定とし、接続時にはトランザクションが開始されるものとする.

	return result, nil
}

func doUpdate(handler *SimpleDBSQLHandler, sql string) (*mysql.Result, error) {
	affectedRows, err := handler.planner.ExecuteUpdate(sql, handler.transaction)
	if err != nil {
		return nil, err
	}

	result := mysql.NewResult(nil)
	result.Status = mysql.SERVER_STATUS_IN_TRANS // AUTOCOMMIT=off がデフォルトの想定とする.
	result.AffectedRows = uint64(affectedRows)
	return result, nil
}

func doTransactionCommand(handler *SimpleDBSQLHandler, sql string) (*mysql.Result, error) {
	parser := parsing.NewParser() // ここで parser 呼ぶのは違う気がするけど...

	sqlData, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}

	switch sqlData.(type) {
	case *data.CommitData:
		defer handler.renewTransaction()
		handler.transaction.Commit()
		result := mysql.NewResult(nil)
		result.Status = mysql.SERVER_STATUS_AUTOCOMMIT
		return result, nil
	case *data.RollbackData:
		defer handler.renewTransaction()
		handler.transaction.Rollback()
		result := mysql.NewResult(nil)
		result.Status = mysql.SERVER_STATUS_AUTOCOMMIT
		return result, nil
	default:
		return nil, NotTransactionCommandError{sql}
	}
}

func buildResultsetFrom(scan query.Scan) (*mysql.Resultset, error) {
	names := buildResultsetNames(scan)
	values, err := buildResultsetValues(scan)

	if err != nil {
		return nil, err
	}

	return mysql.BuildSimpleTextResultset(names, values)
}

func buildResultsetNames(scan query.Scan) []string {
	fieldNames := scan.GetFields()
	names := make([]string, 0, len(fieldNames))

	for _, fieldName := range fieldNames {
		names = append(names, string(fieldName))
	}

	return names
}

func buildResultsetValues(scan query.Scan) ([][]any, error) {
	fieldNames := scan.GetFields()
	values := [][]any{}

	for scan.Next() {
		row := make([]any, 0, len(fieldNames))
		for _, fieldName := range fieldNames {
			value, err := scan.GetValue(fieldName)
			if err != nil {
				return nil, err
			}
			row = append(row, value.GetRawValue())
		}
		values = append(values, row)
	}

	return values, nil
}
