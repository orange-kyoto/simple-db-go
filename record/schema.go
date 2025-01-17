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

func (s *Schema) Add(fieldName types.FieldName, schema *Schema) error {
	fieldType, err := schema.FieldType(fieldName)
	if err != nil {
		return err
	}

	fieldLength, err := schema.Length(fieldName)
	if err != nil {
		return err
	}

	s.AddField(fieldName, fieldType, fieldLength)
	return nil
}

func (s *Schema) AddAll(schema *Schema) {
	for _, fieldName := range schema.Fields() {
		// schema.Fields() から取得した field に対して操作するので、エラーは発生しないはず.
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

func (s *Schema) FieldType(fieldName types.FieldName) (types.FieldType, error) {
	info, exists := s.fieldInfos[fieldName]
	if !exists {
		return 0, &UnknownFieldError{s, fieldName}
	}
	return info.fieldType, nil
}

func (s *Schema) Length(fieldName types.FieldName) (types.FieldLength, error) {
	info, exists := s.fieldInfos[fieldName]
	if !exists {
		return 0, &UnknownFieldError{s, fieldName}
	}
	return info.length, nil
}

func (s *Schema) IsIntField(fieldName types.FieldName) (bool, error) {
	fieldType, err := s.FieldType(fieldName)
	if err != nil {
		return false, err
	}
	return fieldType == constants.INTEGER, nil
}

func (s *Schema) IsStringField(fieldName types.FieldName) (bool, error) {
	fieldType, err := s.FieldType(fieldName)
	if err != nil {
		return false, err
	}
	return fieldType == constants.VARCHAR, nil
}
