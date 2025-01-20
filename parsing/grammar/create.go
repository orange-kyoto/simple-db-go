package grammar

import (
	"simple-db-go/parsing/data"
	"simple-db-go/types"

	"github.com/alecthomas/participle/v2"
)

func FieldDefUnion() participle.Option {
	return participle.Union[FieldDef](IntFieldDef{})
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
	return nil
}

type CreateViewCmd struct {
	ViewName     types.ViewName `"CREATE" "VIEW" @Ident`
	ViewDefQuery Query          `"AS" @@`
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

type FieldDef interface{ GrammarFieldDef() }

type IntFieldDef struct {
	FieldName types.FieldName `@Ident "INT"`
}

func (IntFieldDef) GrammarFieldDef() {}

type VarcharFieldDef struct {
	FieldName   types.FieldName   `@Ident "VARCHAR"`
	FieldLength types.FieldLength `"(" @Int ")"`
}

func (VarcharFieldDef) GrammarFieldDef() {}
