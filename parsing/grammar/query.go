package grammar

import (
	"simple-db-go/parsing/data"
	"simple-db-go/types"
)

type Query struct {
	FieldNames []types.FieldName `"SELECT" @Ident ( "," @Ident )*`
	Queryables []data.Queryable  `"FROM" @Ident ( "," @Ident )*`
	Where      *Predicate        `( "WHERE" @@ ( "AND" @@ )* )? ";"?`
}

type FieldNameList struct {
	Value []types.FieldName `@Ident ( "," @Ident )*`
}

type TableNameList struct {
	Value []types.TableName `@Ident ( "," @Ident )*`
}

func (*Query) GrammarStatement() {}

func (q *Query) ToData() data.SQLData {
	if q.Where == nil {
		return &data.QueryData{
			FieldNames: q.FieldNames,
			Queryables: q.Queryables,
			Predicate:  nil,
		}
	}

	return &data.QueryData{
		FieldNames: q.FieldNames,
		Queryables: q.Queryables,
		Predicate:  q.Where.ToQueryPredicate(),
	}
}
