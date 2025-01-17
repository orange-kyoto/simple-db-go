package query

import (
	"simple-db-go/record"
	"simple-db-go/types"
)

// SimpleDB での Term は、2つの Expression の等価比較(`=`)のみをサポートする.
type Term struct {
	lhs Expression
	rhs Expression
}

func NewTerm(lhs Expression, rhs Expression) *Term {
	return &Term{lhs: lhs, rhs: rhs}
}

func (t *Term) IsSatisfied(scan Scan) (bool, error) {
	lhsValue, err := t.lhs.Evaluate(scan)
	if err != nil {
		return false, err
	}

	rhsValue, err := t.rhs.Evaluate(scan)
	if err != nil {
		return false, err
	}

	return lhsValue == rhsValue, nil
}

func (t *Term) AppliesTo(schema *record.Schema) bool {
	return t.lhs.AppliesTo(schema) && t.rhs.AppliesTo(schema)
}

// Query Planner の助けになるメソッド.
// いつインデックスを使うべきかを判断するために使う.
// 詳細は Chapter15 で.
func (t *Term) EquatesWithConstant(fieldName types.FieldName) (record.Constant, error) {
	if lhs, ok := t.lhs.(*FieldNameExpression); ok && lhs.fieldName == fieldName {
		if rhs, ok := t.rhs.(*ConstExpression); ok {
			return rhs.value, nil
		}
	}

	if rhs, ok := t.rhs.(*FieldNameExpression); ok && rhs.fieldName == fieldName {
		if lhs, ok := t.lhs.(*ConstExpression); ok {
			return lhs.value, nil
		}
	}

	return nil, &TermCannnotEquatesWithConstantError{t.lhs, t.rhs}
}

// Query Planner の助けになるメソッド.
// いつインデックスを使うべきかを判断するために使う.
// 詳細は Chapter15 で.
func (t *Term) EquatesWithFieldName(fieldName types.FieldName) (types.FieldName, error) {
	if lhs, ok := t.lhs.(*FieldNameExpression); ok && lhs.fieldName == fieldName {
		if rhs, ok := t.rhs.(*FieldNameExpression); ok {
			return rhs.fieldName, nil
		}
	}

	if rhs, ok := t.rhs.(*FieldNameExpression); ok && rhs.fieldName == fieldName {
		if lhs, ok := t.lhs.(*FieldNameExpression); ok {
			return lhs.fieldName, nil
		}
	}

	return "", &TermCannnotEquatesWithFieldNameError{t.lhs, t.rhs}
}

func (t *Term) ToString() string {
	return t.lhs.ToString() + " = " + t.rhs.ToString()
}
