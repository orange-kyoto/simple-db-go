package query

import "simple-db-go/record"

// SimpleDB の Expression は、定数値またはフィールド名のいずれかを表す.
// つまり、IntConstant, StrConstant, FieldNameExpression が共通した振る舞いを持つことを意図する.
type Expression interface {
	Evaluate(scan Scan) (Constant, error)
	AppliesTo(schema *record.Schema) bool
	ToString() string
}

// IntConstant, StrConstant に共通した振る舞いを持たせることを意図する.
type Constant interface {
	Constant()

	// string or types.Int を返すことを意図している（雑な実装ではある）
	GetValue() any

	// string or types.Int を返すことを意図している（雑な実装ではある）
	GetRawValue() any

	ToString() string

	HashCode() uint32
}
