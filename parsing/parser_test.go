package parsing

import (
	"simple-db-go/parsing/data"
	"simple-db-go/query"
	"simple-db-go/record"
	"simple-db-go/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParserParseQuery(t *testing.T) {
	parser := NewParser()

	tests := []struct {
		sql      string
		expected *data.QueryData
	}{
		{
			`SELECT id FROM users`,
			&data.QueryData{
				FieldNames: []types.FieldName{"id"},
				TableNames: []types.TableName{"users"},
				Predicate:  nil,
			},
		},
		{
			`select id, name, age from users, orders;`,
			&data.QueryData{
				FieldNames: []types.FieldName{"id", "name", "age"},
				TableNames: []types.TableName{"users", "orders"},
				Predicate:  nil,
			},
		},
		{
			`SELECT id, name, age FROM users WHERE name = 'hoge'`,
			&data.QueryData{
				FieldNames: []types.FieldName{"id", "name", "age"},
				TableNames: []types.TableName{"users"},
				Predicate: query.NewPredicateWith(
					query.NewTerm(
						query.NewFieldNameExpression("name"),
						query.NewConstExpression(record.NewStrConstant("hoge")),
					),
				),
			},
		},
		{
			`SELECT id, name, age FROM users WHERE id = 1;`,
			&data.QueryData{
				FieldNames: []types.FieldName{"id", "name", "age"},
				TableNames: []types.TableName{"users"},
				Predicate: query.NewPredicateWith(
					query.NewTerm(
						query.NewFieldNameExpression("id"),
						query.NewConstExpression(record.NewIntConstant(1)),
					),
				),
			},
		},
		{
			`SELECT id, name, age FROM users WHERE id = 1 AND name = 'hoge'`,
			&data.QueryData{
				FieldNames: []types.FieldName{"id", "name", "age"},
				TableNames: []types.TableName{"users"},
				Predicate: query.NewPredicateFrom(
					[]*query.Term{
						query.NewTerm(
							query.NewFieldNameExpression("id"),
							query.NewConstExpression(record.NewIntConstant(1)),
						),
						query.NewTerm(
							query.NewFieldNameExpression("name"),
							query.NewConstExpression(record.NewStrConstant("hoge")),
						),
					},
				),
			},
		},
	}

	for i, test := range tests {
		result, err := parser.Parse(test.sql)
		if assert.NoErrorf(t, err, "[i=%d] パースエラーが起きないこと.") {
			assert.IsTypef(t, &data.QueryData{}, result, "[i=%d] result が *Query であること.", i)
			assert.Equalf(t, test.expected, result, "[i=%d] QueryData が期待通りであること.", i)
		}
	}
}

func TestParserParseInsert(t *testing.T) {
	parser := NewParser()

	tests := []struct {
		sql      string
		expected *data.InsertData
	}{
		{
			`INSERT INTO users (id) VALUES (1);`,
			&data.InsertData{
				TableName:  "users",
				FieldNames: []types.FieldName{"id"},
				Values: []record.Constant{
					record.NewIntConstant(1),
				},
			},
		},
		{
			`insert into users (id, name, age) values (1, 'hoge', 20)`,
			&data.InsertData{
				TableName:  "users",
				FieldNames: []types.FieldName{"id", "name", "age"},
				Values: []record.Constant{
					record.NewIntConstant(1),
					record.NewStrConstant("hoge"),
					record.NewIntConstant(20),
				},
			},
		},
	}

	for i, test := range tests {
		result, err := parser.Parse(test.sql)
		if assert.NoErrorf(t, err, "[i=%d] パースエラーが起きないこと.", i) {
			assert.IsTypef(t, &data.InsertData{}, result, "[i=%d] result が *InsertData であること.", i)
			assert.Equalf(t, test.expected, result, "[i=%d] InsertData が期待通りであること.", i)
		}
	}
}

func TestParserParseDelete(t *testing.T) {
	parser := NewParser()

	tests := []struct {
		sql      string
		expected *data.DeleteData
	}{
		{
			`DELETE FROM users;`,
			&data.DeleteData{
				TableName: types.TableName("users"),
				Predicate: nil,
			},
		},
		{
			`delete from orders where id = 1`,
			&data.DeleteData{
				TableName: types.TableName("orders"),
				Predicate: query.NewPredicateWith(
					query.NewTerm(
						query.NewFieldNameExpression("id"),
						query.NewConstExpression(record.NewIntConstant(1)),
					),
				),
			},
		},
		{
			`delete from menus WHERE id = 1 AND name = 'fuga'`,
			&data.DeleteData{
				TableName: types.TableName("menus"),
				Predicate: query.NewPredicateFrom(
					[]*query.Term{
						query.NewTerm(
							query.NewFieldNameExpression("id"),
							query.NewConstExpression(record.NewIntConstant(1)),
						),
						query.NewTerm(
							query.NewFieldNameExpression("name"),
							query.NewConstExpression(record.NewStrConstant("fuga")),
						),
					},
				),
			},
		},
	}

	for i, test := range tests {
		result, err := parser.Parse(test.sql)
		if assert.NoErrorf(t, err, "[i=%d] パースエラーが起きないこと.", i) {
			assert.IsTypef(t, &data.DeleteData{}, result, "[i=%d] result が *DeleteData であること.", i)
			assert.Equalf(t, test.expected, result, "[i=%d] DeleteData が期待通りであること.", i)
		}
	}
}

func TestParserParseModify(t *testing.T) {
	parser := NewParser()

	tests := []struct {
		sql      string
		expected *data.ModifyData
	}{
		{
			`UPDATE users SET name = 'hoge';`,
			&data.ModifyData{
				TableName: types.TableName("users"),
				FieldName: types.FieldName("name"),
				NewValue:  query.NewConstExpression(record.NewStrConstant("hoge")),
				Predicate: nil,
			},
		},
		{
			`update orders set quantity = 10 WHERE id = 1`,
			&data.ModifyData{
				TableName: types.TableName("orders"),
				FieldName: types.FieldName("quantity"),
				NewValue:  query.NewConstExpression(record.NewIntConstant(10)),
				Predicate: query.NewPredicateWith(
					query.NewTerm(
						query.NewFieldNameExpression("id"),
						query.NewConstExpression(record.NewIntConstant(1)),
					),
				),
			},
		},
		{
			`update menus set tag = 'piyo' WHERE id = 1 and name = 'fuga'`,
			&data.ModifyData{
				TableName: types.TableName("menus"),
				FieldName: types.FieldName("tag"),
				NewValue:  query.NewConstExpression(record.NewStrConstant("piyo")),
				Predicate: query.NewPredicateFrom(
					[]*query.Term{
						query.NewTerm(
							query.NewFieldNameExpression("id"),
							query.NewConstExpression(record.NewIntConstant(1)),
						),
						query.NewTerm(
							query.NewFieldNameExpression("name"),
							query.NewConstExpression(record.NewStrConstant("fuga")),
						),
					},
				),
			},
		},
	}

	for i, test := range tests {
		result, err := parser.Parse(test.sql)
		if assert.NoErrorf(t, err, "[i=%d] パースエラーが起きないこと.", i) {
			assert.IsTypef(t, &data.ModifyData{}, result, "[i=%d] result が *ModifyData であること.", i)
			assert.Equalf(t, test.expected, result, "[i=%d] ModifyData が期待通りであること.", i)
		}
	}
}
