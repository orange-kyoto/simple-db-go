package query

import (
	"simple-db-go/record"
	"simple-db-go/types"
	"strings"
)

// SimpleDB では Term の論理積（conjunction, `AND`）のみをサポートする.
type Predicate struct {
	terms []*Term
}

func NewPredicate() *Predicate {
	return &Predicate{terms: make([]*Term, 0)}
}

func NewPredicateWith(term *Term) *Predicate {
	return &Predicate{terms: []*Term{term}}
}

func (p *Predicate) ConjoinWith(other Predicate) {
	p.terms = append(p.terms, other.terms...)
}

func (p *Predicate) IsSatisfied(scan Scan) (bool, error) {
	for _, term := range p.terms {
		isSatisfied, err := term.IsSatisfied(scan)
		if err != nil {
			return false, err
		}
		if !isSatisfied {
			return false, nil
		}
	}
	return true, nil
}

func (p *Predicate) SelectSubPred(schema *record.Schema) (*Predicate, error) {
	result := NewPredicate()

	for _, term := range p.terms {
		if term.AppliesTo(schema) {
			result.terms = append(result.terms, term)
		}
	}

	if len(result.terms) == 0 {
		return nil, &NotFoundSubPredicateError{p, schema}
	}

	return result, nil
}

func (p *Predicate) JoinSubPred(schema1 *record.Schema, schema2 *record.Schema) (*Predicate, error) {
	result := NewPredicate()

	newSchema := record.NewSchema()
	newSchema.AddAll(schema1)
	newSchema.AddAll(schema2)

	for _, term := range p.terms {
		if !term.AppliesTo(schema1) && !term.AppliesTo(schema2) && term.AppliesTo(newSchema) {
			result.terms = append(result.terms, term)
		}
	}

	if len(result.terms) == 0 {
		return nil, &CannotJoinSubPredicateError{p, schema1, schema2}
	}

	return result, nil
}

func (p *Predicate) EquatesWithConstant(fieldName types.FieldName) (record.Constant, error) {
	for _, term := range p.terms {
		if constant, err := term.EquatesWithConstant(fieldName); err == nil {
			return constant, nil
		}
	}
	return nil, &PredicateEquatesWithConstantError{p, fieldName}
}

func (p *Predicate) EquatesWithFieldName(fieldName types.FieldName) (types.FieldName, error) {
	for _, term := range p.terms {
		if fieldName, err := term.EquatesWithFieldName(fieldName); err == nil {
			return fieldName, nil
		}
	}
	return "", &PredicateEquatesWithFieldNameError{p, fieldName}
}

func (p *Predicate) ToString() string {
	termStrings := make([]string, len(p.terms))
	for _, termStr := range termStrings {
		termStrings = append(termStrings, termStr)
	}
	return strings.Join(termStrings, " AND ")
}
