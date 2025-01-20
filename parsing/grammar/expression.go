package grammar

import (
	"simple-db-go/query"
	"simple-db-go/types"

	"github.com/alecthomas/participle/v2"
)

func ExpressionUnion() participle.Option {
	return participle.Union[GrammarExpression](IntConstant{}, StrConstant{}, FieldNameExpression{})
}

type GrammarExpression interface {
	GrammarExpression()
	ToQueryExpression() query.Expression
}

type FieldNameExpression struct {
	Value types.FieldName `@Ident`
}

func (FieldNameExpression) GrammarExpression() {}

func (f FieldNameExpression) ToQueryExpression() query.Expression {
	return query.NewFieldNameExpression(f.Value)
}
