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
		inputSql     string
		expectedData *data.QueryData
		expectedSql  string
	}{
		{
			`SELECT id FROM users`,
			&data.QueryData{
				FieldNames: []types.FieldName{"id"},
				Queryables: []data.Queryable{"users"},
				Predicate:  nil,
			},
			`SELECT id FROM users;`,
		},
		{
			`select id, name, age from users, orders;`,
			&data.QueryData{
				FieldNames: []types.FieldName{"id", "name", "age"},
				Queryables: []data.Queryable{"users", "orders"},
				Predicate:  nil,
			},
			`SELECT id, name, age FROM users, orders;`,
		},
		{
			`SELECT id, name, age FROM users WHERE name = 'hoge'`,
			&data.QueryData{
				FieldNames: []types.FieldName{"id", "name", "age"},
				Queryables: []data.Queryable{"users"},
				Predicate: query.NewPredicateWith(
					query.NewTerm(
						query.NewFieldNameExpression("name"),
						query.NewStrConstant("hoge"),
					),
				),
			},
			`SELECT id, name, age FROM users WHERE name = 'hoge';`,
		},
		{
			`SELECT id, name, age FROM users WHERE id = 1;`,
			&data.QueryData{
				FieldNames: []types.FieldName{"id", "name", "age"},
				Queryables: []data.Queryable{"users"},
				Predicate: query.NewPredicateWith(
					query.NewTerm(
						query.NewFieldNameExpression("id"),
						query.NewIntConstant(1),
					),
				),
			},
			`SELECT id, name, age FROM users WHERE id = 1;`,
		},
		{
			`SELECT id, name, age FROM users WHERE id = 1 AND name = 'hoge'`,
			&data.QueryData{
				FieldNames: []types.FieldName{"id", "name", "age"},
				Queryables: []data.Queryable{"users"},
				Predicate: query.NewPredicateFrom(
					[]*query.Term{
						query.NewTerm(
							query.NewFieldNameExpression("id"),
							query.NewIntConstant(1),
						),
						query.NewTerm(
							query.NewFieldNameExpression("name"),
							query.NewStrConstant("hoge"),
						),
					},
				),
			},
			`SELECT id, name, age FROM users WHERE id = 1 AND name = 'hoge';`,
		},
	}

	for i, test := range tests {
		result, err := parser.Parse(test.inputSql)
		if assert.NoErrorf(t, err, "[i=%d] パースエラーが起きないこと.") {
			assert.IsTypef(t, &data.QueryData{}, result, "[i=%d] result が *Query であること.", i)
			assert.Equalf(t, test.expectedData, result, "[i=%d] QueryData が期待通りであること.", i)
			assert.Equal(t, test.expectedSql, result.(*data.QueryData).ToString(), "[i=%d] ToString() で元の SQL 文が復元できること.", i)
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
				Values:     []query.Constant{query.NewIntConstant(1)},
			},
		},
		{
			`insert into users (id, name, age) values (1, 'hoge', 20)`,
			&data.InsertData{
				TableName:  "users",
				FieldNames: []types.FieldName{"id", "name", "age"},
				Values: []query.Constant{
					query.NewIntConstant(1),
					query.NewStrConstant("hoge"),
					query.NewIntConstant(20),
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
				TableName: "users",
				Predicate: nil,
			},
		},
		{
			`delete from orders where id = 1`,
			&data.DeleteData{
				TableName: "orders",
				Predicate: query.NewPredicateWith(
					query.NewTerm(
						query.NewFieldNameExpression("id"),
						query.NewIntConstant(1),
					),
				),
			},
		},
		{
			`delete from menus WHERE id = 1 AND name = 'fuga'`,
			&data.DeleteData{
				TableName: "menus",
				Predicate: query.NewPredicateFrom(
					[]*query.Term{
						query.NewTerm(
							query.NewFieldNameExpression("id"),
							query.NewIntConstant(1),
						),
						query.NewTerm(
							query.NewFieldNameExpression("name"),
							query.NewStrConstant("fuga"),
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
				TableName: "users",
				FieldName: "name",
				NewValue:  query.NewStrConstant("hoge"),
				Predicate: nil,
			},
		},
		{
			`update orders set quantity = 10 WHERE id = 1`,
			&data.ModifyData{
				TableName: "orders",
				FieldName: "quantity",
				NewValue:  query.NewIntConstant(10),
				Predicate: query.NewPredicateWith(
					query.NewTerm(
						query.NewFieldNameExpression("id"),
						query.NewIntConstant(1),
					),
				),
			},
		},
		{
			`update menus set tag = 'piyo' where id = 1 and name = 'fuga'`,
			&data.ModifyData{
				TableName: "menus",
				FieldName: "tag",
				NewValue:  query.NewStrConstant("piyo"),
				Predicate: query.NewPredicateFrom(
					[]*query.Term{
						query.NewTerm(
							query.NewFieldNameExpression("id"),
							query.NewIntConstant(1),
						),
						query.NewTerm(
							query.NewFieldNameExpression("name"),
							query.NewStrConstant("fuga"),
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

func TestParserParseCreateTable(t *testing.T) {
	parser := NewParser()

	tests := []struct {
		sql      string
		expected *data.CreateTableData
	}{
		{
			`CREATE TABLE users (id INT, name VARCHAR(255));`,
			func() *data.CreateTableData {
				schema := record.NewSchema()
				schema.AddIntField("id")
				schema.AddStringField("name", 255)
				return &data.CreateTableData{TableName: "users", Schema: schema}
			}(),
		},
		{
			`CREATE TABLE orders (id INT, name VARCHAR(255), quantity INT);`,
			func() *data.CreateTableData {
				schema := record.NewSchema()
				schema.AddIntField("id")
				schema.AddStringField("name", 255)
				schema.AddIntField("quantity")
				return &data.CreateTableData{TableName: "orders", Schema: schema}
			}(),
		},
		{
			`create table users (id int, name varchar(255));`,
			func() *data.CreateTableData {
				schema := record.NewSchema()
				schema.AddIntField("id")
				schema.AddStringField("name", 255)
				return &data.CreateTableData{TableName: "users", Schema: schema}
			}(),
		},
	}

	for i, test := range tests {
		result, err := parser.Parse(test.sql)
		if assert.NoErrorf(t, err, "[i=%d] パースエラーが起きないこと.", i) {
			assert.IsTypef(t, &data.CreateTableData{}, result, "[i=%d] result が *CreateTableData であること.", i)
			assert.Equalf(t, test.expected, result, "[i=%d] CreateTableData が期待通りであること.", i)
		}
	}
}

func TestParserParseCreateView(t *testing.T) {
	parser := NewParser()

	tests := []struct {
		inputSql        string
		expectedData    *data.CreateViewData
		expectedViewDef types.ViewDef
	}{
		{
			`CREATE VIEW view1 AS SELECT id, name FROM users;`,
			&data.CreateViewData{
				ViewName: "view1",
				QueryData: &data.QueryData{
					FieldNames: []types.FieldName{"id", "name"},
					Queryables: []data.Queryable{"users"},
					Predicate:  nil,
				},
			},
			`SELECT id, name FROM users;`,
		},
		{
			`CREATE VIEW view2 AS SELECT id, name, age FROM users WHERE age = 20;`,
			&data.CreateViewData{
				ViewName: "view2",
				QueryData: &data.QueryData{
					FieldNames: []types.FieldName{"id", "name", "age"},
					Queryables: []data.Queryable{"users"},
					Predicate: query.NewPredicateWith(
						query.NewTerm(
							query.NewFieldNameExpression("age"),
							query.NewIntConstant(20),
						),
					),
				},
			},
			`SELECT id, name, age FROM users WHERE age = 20;`,
		},
		{
			`create view view1 as select id, name from users;`,
			&data.CreateViewData{
				ViewName: "view1",
				QueryData: &data.QueryData{
					FieldNames: []types.FieldName{"id", "name"},
					Queryables: []data.Queryable{"users"},
					Predicate:  nil,
				},
			},
			`SELECT id, name FROM users;`,
		},
	}

	for i, test := range tests {
		result, err := parser.Parse(test.inputSql)
		if assert.NoErrorf(t, err, "[i=%d] パースエラーが起きないこと.", i) {
			assert.IsTypef(t, &data.CreateViewData{}, result, "[i=%d] result が *CreateViewData であること.", i)
			assert.Equalf(t, test.expectedData, result, "[i=%d] CreateViewData が期待通りであること.", i)
			assert.Equalf(t, test.expectedViewDef, result.(*data.CreateViewData).GetViewDef(), "[i=%d] GetViewDef でビュー定義だけを取得できること.", i)
		}
	}
}

func TestParserParseCreateIndex(t *testing.T) {
	parser := NewParser()

	tests := []struct {
		sql      string
		expected *data.CreateIndexData
	}{
		{
			`CREATE INDEX idx1 ON users (id);`,
			&data.CreateIndexData{
				IndexName: "idx1",
				TableName: "users",
				FieldName: "id",
			},
		},
		{
			`create index idx1 on users (id);`,
			&data.CreateIndexData{
				IndexName: "idx1",
				TableName: "users",
				FieldName: "id",
			},
		},
	}

	for i, test := range tests {
		result, err := parser.Parse(test.sql)
		if assert.NoErrorf(t, err, "[i=%d] パースエラーが起きないこと.", i) {
			assert.IsTypef(t, &data.CreateIndexData{}, result, "[i=%d] result が *CreateIndexData であること.", i)
			assert.Equalf(t, test.expected, result, "[i=%d] CreateIndexData が期待通りであること.", i)
		}
	}
}

func TestParserParseCommit(t *testing.T) {
	parser := NewParser()

	tests := []struct {
		sql      string
		expected *data.CommitData
	}{
		{`COMMIT;`, &data.CommitData{}},
		{`COMMIT`, &data.CommitData{}},
		{`commit;`, &data.CommitData{}},
		{`commit`, &data.CommitData{}},
	}

	for i, test := range tests {
		result, err := parser.Parse(test.sql)
		if assert.NoErrorf(t, err, "[i=%d] パースエラーが起きないこと.", i) {
			assert.IsTypef(t, &data.CommitData{}, result, "[i=%d] result が *CommitData であること.", i)
			assert.Equalf(t, test.expected, result, "[i=%d] CommitData が期待通りであること.", i)
		}
	}
}

func TestParserParseRollback(t *testing.T) {
	parser := NewParser()

	tests := []struct {
		sql      string
		expected *data.RollbackData
	}{
		{`ROLLBACK;`, &data.RollbackData{}},
		{`ROLLBACK`, &data.RollbackData{}},
		{`rollback;`, &data.RollbackData{}},
		{`rollback`, &data.RollbackData{}},
	}

	for i, test := range tests {
		result, err := parser.Parse(test.sql)
		if assert.NoErrorf(t, err, "[i=%d] パースエラーが起きないこと.", i) {
			assert.IsTypef(t, &data.RollbackData{}, result, "[i=%d] result が *RollbackData であること.", i)
			assert.Equalf(t, test.expected, result, "[i=%d] RollbackData が期待通りであること.", i)
		}
	}
}
