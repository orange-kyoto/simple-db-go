package data

import (
	"fmt"
	"simple-db-go/query"
	"simple-db-go/types"
	"strings"
)

type QueryData struct {
	FieldNames []types.FieldName
	Queryables []Queryable
	Predicate  *query.Predicate
}

func (*QueryData) SQLData() {}

func (q *QueryData) ToString() string {
	fieldNames := make([]string, 0, len(q.FieldNames))
	for _, fieldName := range q.FieldNames {
		fieldNames = append(fieldNames, string(fieldName))
	}

	queryables := make([]string, 0, len(q.Queryables))
	for _, tableName := range q.Queryables {
		queryables = append(queryables, string(tableName.ToString()))
	}

	if q.Predicate == nil {
		return fmt.Sprintf(
			"SELECT %s FROM %s;",
			strings.Join(fieldNames, ", "),
			strings.Join(queryables, ", "),
		)
	}

	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s;",
		strings.Join(fieldNames, ", "),
		strings.Join(queryables, ", "),
		q.Predicate.ToString(),
	)
}
