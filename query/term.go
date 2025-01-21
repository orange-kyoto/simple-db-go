package query

import (
	"fmt"
	"simple-db-go/constants"
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

// Term が `someFiled = 'hoge'`のような、フィールドを定数値で比較する形式になっているか判断する.
// planning でコストを計算する際に、ある列の異なる値の数を推定する際に用いる.
func (t *Term) EquatesWithConstant(fieldName types.FieldName) (Constant, error) {
	if lhs, ok := t.lhs.(FieldNameExpression); ok && lhs.fieldName == fieldName {
		if rhs, ok := t.rhs.(Constant); ok {
			return rhs, nil
		}
	}

	if rhs, ok := t.rhs.(FieldNameExpression); ok && rhs.fieldName == fieldName {
		if lhs, ok := t.lhs.(Constant); ok {
			return lhs, nil
		}
	}

	return nil, &TermCannnotEquatesWithConstantError{t.lhs, t.rhs}
}

// Query Planner の助けになるメソッド.
// いつインデックスを使うべきかを判断するために使う.
// 詳細は Chapter15 で.
func (t *Term) EquatesWithFieldName(fieldName types.FieldName) (types.FieldName, error) {
	if lhs, ok := t.lhs.(FieldNameExpression); ok && lhs.fieldName == fieldName {
		if rhs, ok := t.rhs.(FieldNameExpression); ok {
			return rhs.fieldName, nil
		}
	}

	if rhs, ok := t.rhs.(FieldNameExpression); ok && rhs.fieldName == fieldName {
		if lhs, ok := t.lhs.(FieldNameExpression); ok {
			return lhs.fieldName, nil
		}
	}

	return "", &TermCannnotEquatesWithFieldNameError{t.lhs, t.rhs}
}

func (t *Term) ToString() string {
	return t.lhs.ToString() + " = " + t.rhs.ToString()
}

func (t *Term) GetReductionFactor(plan Plan) types.Int {
	switch lhs := t.lhs.(type) {
	case FieldNameExpression:
		{
			switch rhs := t.rhs.(type) {
			case FieldNameExpression:
				{
					return max(
						plan.GetDistinctValues(lhs.GetFieldName()),
						plan.GetDistinctValues(rhs.GetFieldName()),
					)
				}
			case Constant:
				{
					return plan.GetDistinctValues(lhs.GetFieldName())
				}
			default:
				panic(fmt.Sprintf("Unexpected type: %T", rhs))
			}
		}
	case Constant:
		{
			switch rhs := t.rhs.(type) {
			case FieldNameExpression:
				{
					return plan.GetDistinctValues(rhs.GetFieldName())
				}
			case Constant:
				{
					if lhs == rhs {
						// NOTE: 定数として一致した場合、この term は何もレコードをフィルタリングしない.
						return 1
					} else {
						// NOTE: 定数として一致しない場合、この term は全てのレコードをフィルタリングする.
						return constants.MAX_INT_VALUE
					}
				}
			default:
				panic(fmt.Sprintf("Unexpected type: %T", rhs))
			}
		}
	default:
		panic(fmt.Sprintf("Unexpected type: %T", lhs))
	}
}
