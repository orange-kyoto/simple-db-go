package grammar

import (
	"simple-db-go/parsing/data"

	"github.com/alecthomas/participle/v2"
)

type Statement interface {
	GrammarStatement()
	ToData() data.SQLData
}

func StatementUnion() participle.Option {
	return participle.Union[Statement](
		&Query{},
		&InsertCmd{},
		&DeleteCmd{},
		&ModifyCmd{},
		&CreateTableCmd{},
		&CreateViewCmd{},
		&CreateIndexCmd{},
	)
}
