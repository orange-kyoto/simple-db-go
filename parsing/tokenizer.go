package parsing

import (
	"github.com/bzick/tokenizer"
)

type Tokenizer struct {
	parser *tokenizer.Tokenizer
}

func NewTokenizer() *Tokenizer {
	parser := tokenizer.New()
	parser.DefineTokens(Delimiter, []string{",", "=", "."})
	parser.DefineTokens(Keyword, simpleDBKeywordList)
	parser.DefineStringToken(StrConstant, "'", "'").SetEscapeSymbol(tokenizer.BackSlash)
	parser.AllowKeywordSymbols(tokenizer.Underscore, tokenizer.Numbers)

	return &Tokenizer{parser}
}

func (t *Tokenizer) stream(input string) *tokenizer.Stream {
	return t.parser.ParseString(input)
}
