package query

import (
	"simple-db-go/record"
	"simple-db-go/types"
)

type Expression struct {
	constant  Constant
	fieldName types.FieldName
	isConst   bool
}

func NewConstExpression(value Constant) *Expression {
	return &Expression{constant: value, isConst: true}
}

func NewFieldExpression(fieldName types.FieldName) *Expression {
	return &Expression{fieldName: fieldName, isConst: false}
}

func (e *Expression) IsFieldName() bool {
	return !e.isConst
}

func (e *Expression) AsFieldName() types.FieldName {
	return e.fieldName
}

func (e *Expression) AsConstant() Constant {
	return e.constant
}

func (e *Expression) Evaluate(scan Scan) Constant {
	if e.isConst {
		return e.constant
	}
	return scan.GetValue(e.fieldName)
}

func (e *Expression) AppliesTo(schema record.Schema) bool {
	if e.isConst {
		return true
	}
	return schema.HasField(e.fieldName)
}

func (e *Expression) ToString() string {
	if e.isConst {
		return e.constant.ToString()
	}
	return string(e.fieldName)
}
