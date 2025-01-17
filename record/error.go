package record

import (
	"fmt"
	"simple-db-go/types"
)

type UnknownFieldError struct {
	schema    *Schema
	fieldName types.FieldName
}

func (e *UnknownFieldError) Error() string {
	return fmt.Sprintf("Layoutに存在しないフィールドが指定されました。schema=%+v, fieldName=%s", e.schema, e.fieldName)
}
