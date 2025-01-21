package grammar

import (
	"simple-db-go/parsing/data"
	"simple-db-go/query"
	"simple-db-go/types"

	"github.com/alecthomas/participle/v2"
)

func UpdateCmdUnion() participle.Option {
	return participle.Union[UpdateCmd](
		&InsertCmd{},
		&DeleteCmd{},
		&ModifyCmd{},
		&CreateTableCmd{},
		&CreateViewCmd{},
		&CreateIndexCmd{},
	)
}

type UpdateCmd interface{ GrammarUpdateCmd() }

type InsertCmd struct {
	TableName  types.TableName   `"INSERT" "INTO" @Ident`
	FieldNames []types.FieldName `"(" @Ident ( "," @Ident )* ")"`
	Constants  []Constant        `"VALUES" "(" @@ ( "," @@ )* ")" ";"?`
}

func (*InsertCmd) GrammarUpdateCmd() {}
func (*InsertCmd) GrammarStatement() {}
func (i *InsertCmd) ToData() data.SQLData {
	values := make([]query.Constant, 0, len(i.Constants))
	for _, c := range i.Constants {
		values = append(values, c.ToQueryConstant())
	}

	return &data.InsertData{
		TableName:  i.TableName,
		FieldNames: i.FieldNames,
		Values:     values,
	}
}

type DeleteCmd struct {
	TableName types.TableName `"DELETE" "FROM" @Ident`
	Where     *Predicate      `( "WHERE" @@ ( "AND" @@ )* )? ";"?`
}

func (*DeleteCmd) GrammarUpdateCmd() {}
func (*DeleteCmd) GrammarStatement() {}
func (d *DeleteCmd) ToData() data.SQLData {
	if d.Where == nil {
		return &data.DeleteData{
			TableName: d.TableName,
			Predicate: nil,
		}
	}

	return &data.DeleteData{
		TableName: d.TableName,
		Predicate: d.Where.ToQueryPredicate(),
	}
}

type ModifyCmd struct {
	TableName  types.TableName   `"UPDATE" @Ident`
	FieldName  types.FieldName   `"SET" @Ident "="`
	Expression GrammarExpression `@@`
	Where      *Predicate        `( "WHERE" @@ ( "AND" @@ )* )? ";"?`
}

func (*ModifyCmd) GrammarUpdateCmd() {}
func (*ModifyCmd) GrammarStatement() {}
func (m *ModifyCmd) ToData() data.SQLData {
	if m.Where == nil {
		return &data.ModifyData{
			TableName: m.TableName,
			FieldName: m.FieldName,
			NewValue:  m.Expression.ToQueryExpression(),
			Predicate: nil,
		}
	}

	return &data.ModifyData{
		TableName: m.TableName,
		FieldName: m.FieldName,
		NewValue:  m.Expression.ToQueryExpression(),
		Predicate: m.Where.ToQueryPredicate(),
	}
}
