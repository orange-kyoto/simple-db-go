package grammar

import (
	"simple-db-go/query"
	"simple-db-go/types"

	"github.com/alecthomas/participle/v2"
)

func ConstantUnion() participle.Option {
	return participle.Union[Constant](IntConstant{}, StrConstant{})
}

type Constant interface {
	GrammarConstant()
	ToQueryConstant() query.Constant
}

type IntConstant struct {
	Value types.Int `@Int`
}

func (i IntConstant) GrammarExpression()                  {}
func (i IntConstant) ToQueryExpression() query.Expression { return query.NewIntConstant(i.Value) }
func (i IntConstant) GrammarConstant()                    {}
func (i IntConstant) ToQueryConstant() query.Constant     { return query.NewIntConstant(i.Value) }

type StrConstant struct {
	Value string `@String`
}

func (s StrConstant) GrammarExpression()                  {}
func (s StrConstant) ToQueryExpression() query.Expression { return query.NewStrConstant(s.Value) }
func (s StrConstant) GrammarConstant()                    {}
func (s StrConstant) ToQueryConstant() query.Constant     { return query.NewStrConstant(s.Value) }
