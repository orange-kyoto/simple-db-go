package query

import (
	"simple-db-go/record"
	"simple-db-go/types"
)

func NewConstExpression(value record.Constant) *ConstExpression {
	return &ConstExpression{value: value}
}

func NewFieldNameExpression(fieldName types.FieldName) *FieldNameExpression {
	return &FieldNameExpression{fieldName: fieldName}
}

type ConstExpression struct {
	value record.Constant
}

func (e *ConstExpression) Evaluate(scan Scan) (record.Constant, error) {
	return e.value, nil
}

func (e *ConstExpression) AppliesTo(schema *record.Schema) bool {
	return true
}

func (e *ConstExpression) ToString() string {
	return e.value.ToString()
}

type FieldNameExpression struct {
	fieldName types.FieldName
}

func (e *FieldNameExpression) Evaluate(scan Scan) (record.Constant, error) {
	return scan.GetValue(e.fieldName)
}

func (e *FieldNameExpression) AppliesTo(schema *record.Schema) bool {
	return schema.HasField(e.fieldName)
}

func (e *FieldNameExpression) ToString() string {
	return string(e.fieldName)
}
