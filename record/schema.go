package record

import (
	"simple-db-go/constants"
	"simple-db-go/types"
)

// DB record の論理的な構造を表現する構造体.
type Schema struct {
	fields     []types.FieldName
	fieldInfos map[types.FieldName]fieldInfo
}

func NewSchema() *Schema {
	return &Schema{
		fields:     make([]types.FieldName, 0),
		fieldInfos: make(map[types.FieldName]fieldInfo),
	}
}

func (s *Schema) AddField(fieldName types.FieldName, fieldType types.FieldType, length types.FieldLength) {
	s.fields = append(s.fields, fieldName)
	s.fieldInfos[fieldName] = newFieldInfo(fieldType, length)
}

func (s *Schema) AddIntField(fieldName types.FieldName) {
	s.AddField(fieldName, constants.INTEGER, INTEGER_FIELD_LENGTH)
}

func (s *Schema) AddStringField(fieldName types.FieldName, length types.FieldLength) {
	s.AddField(fieldName, constants.VARCHAR, length)
}

func (s *Schema) Add(fieldName types.FieldName, schema Schema) {
	fieldType := schema.FieldType(fieldName)
	fieldLength := schema.Length(fieldName)
	s.AddField(fieldName, fieldType, fieldLength)
}

func (s *Schema) AddAll(schema Schema) {
	for _, fieldName := range schema.Fields() {
		s.Add(fieldName, schema)
	}
}

func (s *Schema) Fields() []types.FieldName {
	return s.fields
}

func (s *Schema) HasField(fieldName types.FieldName) bool {
	for _, f := range s.Fields() {
		if f == fieldName {
			return true
		}
	}
	return false
}

func (s *Schema) FieldType(fieldName types.FieldName) types.FieldType {
	return s.fieldInfos[fieldName].fieldType
}

func (s *Schema) Length(fieldName types.FieldName) types.FieldLength {
	return s.fieldInfos[fieldName].length
}
