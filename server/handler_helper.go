package server

import (
	"simple-db-go/query"

	"github.com/go-mysql-org/go-mysql/mysql"
)

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
