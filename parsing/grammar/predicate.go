package grammar

import (
	"simple-db-go/query"
	"simple-db-go/types"
)

// NOTE: SimpleDB では `AND` で結合された複数の条件をサポートするのみとする.
type Predicate struct {
	Terms []*Term `@@ ( "AND" @@ )*`
}

// NOTE: SimpleDB では `=` だけサポートされる.
type Term struct {
	FieldName  types.FieldName   `@Ident "=" `
	Expression GrammarExpression `@@`
}

func (p *Predicate) ToQueryPredicate() *query.Predicate {
	queryTerms := make([]*query.Term, 0, len(p.Terms))
	for _, grammarTerm := range p.Terms {
		queryTerms = append(queryTerms, query.NewTerm(
			query.NewFieldNameExpression(grammarTerm.FieldName),
			grammarTerm.Expression.ToQueryExpression(),
		))
	}

	return query.NewPredicateFrom(queryTerms)
}
