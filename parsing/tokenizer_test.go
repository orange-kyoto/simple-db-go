package parsing

import (
	"testing"

	"github.com/bzick/tokenizer"
	"github.com/stretchr/testify/assert"
)

func TestTokenizerParsing(t *testing.T) {
	tok := NewTokenizer()

	stream := tok.stream("SELECT a.hoge FROM table1 a WHERE id = 1 AND name = 'foo'")
	defer stream.Close()

	tests := []struct {
		key   tokenizer.TokenKey
		value any
	}{
		{Keyword, "SELECT"},
		{tokenizer.TokenKeyword, "a"},
		{Delimiter, "."},
		{tokenizer.TokenKeyword, "hoge"},
		{Keyword, "FROM"},
		{tokenizer.TokenKeyword, "table1"},
		{tokenizer.TokenKeyword, "a"},
		{Keyword, "WHERE"},
		{tokenizer.TokenKeyword, "id"},
		{Delimiter, "="},
		{tokenizer.TokenInteger, "1"},
		{Keyword, "AND"},
		{tokenizer.TokenKeyword, "name"},
		{Delimiter, "="},
		{tokenizer.TokenString, "'foo'"},
	}

	for _, test := range tests {
		assert.True(t, stream.IsValid(), "ストリームはまだ終端に達していてはならない.")
		assert.Equalf(t, test.key, stream.CurrentToken().Key(), "トークンのキーが一致している必要があります. expected=%s, got=%s", TokenKeyToString(test.key), TokenKeyToString(stream.CurrentToken().Key()))
		assert.Equalf(t, test.value, stream.CurrentToken().ValueString(), "トークンの値が一致している必要があります. expected=%s, got=%s", test.value, stream.CurrentToken().ValueString())
		stream.GoNext()
	}
	assert.False(t, stream.IsValid(), "ストリームは終端に達している必要があります.")
}
