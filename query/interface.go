package query

import "simple-db-go/record"

// SimpleDB の Expression は、定数値またはフィールド名のいずれかを表す.
type Expression interface {
	Evaluate(scan Scan) (record.Constant, error)
	AppliesTo(schema *record.Schema) bool
	ToString() string
}
