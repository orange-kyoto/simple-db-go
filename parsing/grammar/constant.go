package grammar

import (
	"simple-db-go/query"
	"simple-db-go/record"
	"simple-db-go/types"

	"github.com/alecthomas/participle/v2"
)

func ConstantUnion() participle.Option {
	return participle.Union[Constant](IntConstant{}, StrConstant{})
}

type Constant interface {
	GrammarConstant()
	ToRecordConstant() record.Constant
}

type IntConstant struct {
	Value types.Int `@Int`
}

func (IntConstant) GrammarExpression() {}
func (i IntConstant) ToQueryExpression() query.Expression {
	value := record.NewIntConstant(i.Value)
	return query.NewConstExpression(value)
}
func (IntConstant) GrammarConstant() {}
func (i IntConstant) ToRecordConstant() record.Constant {
	return record.NewIntConstant(i.Value)
}

type StrConstant struct {
	Value string `@String`
}

func (StrConstant) GrammarExpression() {}
func (s StrConstant) ToQueryExpression() query.Expression {
	value := record.NewStrConstant(s.Value)
	return query.NewConstExpression(value)
}
func (StrConstant) GrammarConstant() {}
func (s StrConstant) ToRecordConstant() record.Constant {
	return record.NewStrConstant(s.Value)
}
