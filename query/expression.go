package query

import (
	"simple-db-go/record"
	"simple-db-go/types"
)

func NewFieldNameExpression(fieldName types.FieldName) FieldNameExpression {
	return FieldNameExpression{fieldName: fieldName}
}

type FieldNameExpression struct {
	fieldName types.FieldName
}

func (e FieldNameExpression) Evaluate(scan Scan) (Constant, error) {
	return scan.GetValue(e.fieldName)
}

func (e FieldNameExpression) AppliesTo(schema *record.Schema) bool {
	return schema.HasField(e.fieldName)
}

func (e FieldNameExpression) ToString() string {
	return string(e.fieldName)
}

func (e FieldNameExpression) GetFieldName() types.FieldName {
	return e.fieldName
}
