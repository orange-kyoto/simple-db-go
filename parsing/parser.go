package parsing

import (
	"simple-db-go/parsing/data"
	"simple-db-go/parsing/grammar"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

type Parser struct {
	parser *participle.Parser[grammar.SimpleDBSQL]
}

func NewParser() *Parser {
	initLexer := lexer.MustSimple([]lexer.SimpleRule{
		{Name: `Keyword`, Pattern: `(?i)\b(SELECT|FROM|WHERE|AND|AS|CREATE|INSERT|INTO|VALUES|UPDATE|SET|DELETE|INDEX|ON|VIEW|TABLE|INT|VARCHAR|COMMIT|ROLLBACK)\b`},
		{Name: `Ident`, Pattern: `[a-zA-Z][a-zA-Z_\d]*`},
		{Name: `String`, Pattern: `'[^']*'|"[^"]*"`},
		{Name: `Int`, Pattern: `-?[1-9][0-9]*`},
		{Name: `Operators`, Pattern: `[,=;()]`},
		{Name: `whitespace`, Pattern: `\s+`},
	})

	return &Parser{participle.MustBuild[grammar.SimpleDBSQL](
		participle.Lexer(initLexer),
		participle.Unquote("String"),
		participle.CaseInsensitive("Keyword"),
		grammar.ExpressionUnion(),
		grammar.UpdateCmdUnion(),
		grammar.FieldDefUnion(),
		grammar.ConstantUnion(),
		grammar.StatementUnion(),
	)}
}

func (p *Parser) Parse(sql string) (data.SQLData, error) {
	parsedSql, err := p.parser.ParseString("SimpleDB Parser", string(sql))
	if err != nil {
		return nil, err
	}

	return parsedSql.Statement.ToData(), nil
}
