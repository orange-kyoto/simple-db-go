package data

import (
	"fmt"
	"simple-db-go/query"
	"simple-db-go/types"
	"strings"
)

type QueryData struct {
	FieldNames []types.FieldName
	TableNames []types.TableName
	Predicate  *query.Predicate
}

func (*QueryData) SQLData() {}

func (q *QueryData) ToString() string {
	fieldNames := make([]string, 0, len(q.FieldNames))
	for _, fieldName := range q.FieldNames {
		fieldNames = append(fieldNames, string(fieldName))
	}

	tableNames := make([]string, 0, len(q.TableNames))
	for _, tableName := range q.TableNames {
		tableNames = append(tableNames, string(tableName))
	}

	if q.Predicate == nil {
		return fmt.Sprintf(
			"SELECT %s FROM %s;",
			strings.Join(fieldNames, ", "),
			strings.Join(tableNames, ", "),
		)
	}

	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s;",
		strings.Join(fieldNames, ", "),
		strings.Join(tableNames, ", "),
		q.Predicate.ToString(),
	)
}
