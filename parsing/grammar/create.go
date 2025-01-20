package grammar

import (
	"simple-db-go/constants"
	"simple-db-go/parsing/data"
	"simple-db-go/record"
	"simple-db-go/types"

	"github.com/alecthomas/participle/v2"
)

func FieldDefUnion() participle.Option {
	return participle.Union[FieldDef](IntFieldDef{}, VarcharFieldDef{})
}

type CreateCmd interface{ GrammarCreateCmd() }

type CreateTableCmd struct {
	TableName types.TableName `"CREATE" "TABLE" @Ident`
	FieldDefs []FieldDef      `"(" @@ ( "," @@ )* ")" ";"?`
}

func (*CreateTableCmd) GrammarUpdateCmd() {}
func (*CreateTableCmd) GrammarCreateCmd() {}
func (*CreateTableCmd) GrammarStatement() {}
func (c *CreateTableCmd) ToData() data.SQLData {
	schema := record.NewSchema()
	for _, fieldDef := range c.FieldDefs {
		schema.AddField(
			fieldDef.GetFieldName(),
			fieldDef.GetFieldType(),
			fieldDef.GetFieldLength(),
		)
	}

	return &data.CreateTableData{
		TableName: c.TableName,
		Schema:    schema,
	}
}

type CreateViewCmd struct {
	ViewName     types.ViewName `"CREATE" "VIEW" @Ident`
	ViewDefQuery *Query         `"AS" @@`
}

func (*CreateViewCmd) GrammarUpdateCmd()    {}
func (*CreateViewCmd) GrammarCreateCmd()    {}
func (*CreateViewCmd) GrammarStatement()    {}
func (*CreateViewCmd) ToData() data.SQLData { return nil }

type CreateIndexCmd struct {
	IndexName types.IndexName `"CREATE" "INDEX" @Ident`
	TableName types.TableName `"ON" @Ident`
	FieldName types.FieldName `"(" @Ident ")" ";"?`
}

func (*CreateIndexCmd) GrammarUpdateCmd()    {}
func (*CreateIndexCmd) GrammarCreateCmd()    {}
func (*CreateIndexCmd) GrammarStatement()    {}
func (*CreateIndexCmd) ToData() data.SQLData { return nil }

type FieldDef interface {
	GrammarFieldDef()
	GetFieldName() types.FieldName
	GetFieldType() types.FieldType
	GetFieldLength() types.FieldLength
}

type IntFieldDef struct {
	FieldName types.FieldName `@Ident "INT"`
}

func (IntFieldDef) GrammarFieldDef() {}
func (i IntFieldDef) GetFieldName() types.FieldName {
	return i.FieldName
}
func (IntFieldDef) GetFieldType() types.FieldType {
	return constants.INTEGER
}
func (IntFieldDef) GetFieldLength() types.FieldLength {
	return record.INTEGER_FIELD_LENGTH
}

type VarcharFieldDef struct {
	FieldName   types.FieldName   `@Ident "VARCHAR"`
	FieldLength types.FieldLength `"(" @Int ")"`
}

func (VarcharFieldDef) GrammarFieldDef() {}
func (v VarcharFieldDef) GetFieldName() types.FieldName {
	return v.FieldName
}
func (VarcharFieldDef) GetFieldType() types.FieldType {
	return constants.VARCHAR
}
func (v VarcharFieldDef) GetFieldLength() types.FieldLength {
	return v.FieldLength
}
