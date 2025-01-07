package record

// DB record の論理的な構造を表現する構造体.
type Schema struct {
	fields     []FieldName
	fieldInfos map[FieldName]fieldInfo
}

func NewSchema() *Schema {
	return &Schema{
		fields:     make([]FieldName, 0),
		fieldInfos: make(map[FieldName]fieldInfo),
	}
}

func (s *Schema) AddField(fieldName FieldName, fieldType FieldType, length FieldLength) {
	s.fields = append(s.fields, fieldName)
	s.fieldInfos[fieldName] = newFieldInfo(fieldType, length)
}

func (s *Schema) AddIntField(fieldName FieldName) {
	s.AddField(fieldName, INTEGER, INTEGER_FIELD_LENGTH)
}

func (s *Schema) AddStringField(fieldName FieldName, length FieldLength) {
	s.AddField(fieldName, VARCHAR, length)
}

func (s *Schema) Add(fieldName FieldName, schema Schema) {
	fieldType := schema.FieldType(fieldName)
	fieldLength := schema.Length(fieldName)
	s.AddField(fieldName, fieldType, fieldLength)
}

func (s *Schema) AddAll(schema Schema) {
	for _, fieldName := range schema.Fields() {
		s.Add(fieldName, schema)
	}
}

func (s *Schema) Fields() []FieldName {
	return s.fields
}

func (s *Schema) HasField(fieldName FieldName) bool {
	for _, f := range s.Fields() {
		if f == fieldName {
			return true
		}
	}
	return false
}

func (s *Schema) FieldType(fieldName FieldName) FieldType {
	return s.fieldInfos[fieldName].fieldType
}

func (s *Schema) Length(fieldName FieldName) FieldLength {
	return s.fieldInfos[fieldName].length
}
