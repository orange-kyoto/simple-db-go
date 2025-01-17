package parsing

import (
	"simple-db-go/types"

	"github.com/bzick/tokenizer"
)

type Lexer struct {
	streamTokenizer *tokenizer.Stream
}

func NewLexer(input string) *Lexer {
	tokenizer := NewTokenizer()
	stream := tokenizer.stream(input)
	lexer := &Lexer{stream}

	return lexer
}

func (l *Lexer) MatchDelimiter(d rune) bool {
	return l.streamTokenizer.CurrentToken().Key() == Delimiter && l.streamTokenizer.CurrentToken().ValueString() == string(d)
}

func (l *Lexer) MatchIntConstant() bool {
	return l.streamTokenizer.CurrentToken().Key() == tokenizer.TokenInteger
}

func (l *Lexer) MatchStrConstant() bool {
	return l.streamTokenizer.CurrentToken().Key() == StrConstant
}

func (l *Lexer) MatchKeyword(keyword string) bool {
	return l.streamTokenizer.CurrentToken().Key() == Keyword && l.streamTokenizer.CurrentToken().ValueString() == keyword
}

func (l *Lexer) MatchIdentifier() bool {
	isKeywordType := l.streamTokenizer.CurrentToken().Key() == tokenizer.TokenKeyword
	_, exists := simpleDBKeywords[l.streamTokenizer.CurrentToken().ValueString()]
	return isKeywordType && exists
}

func (l *Lexer) EatDelimiter(d rune) error {
	if l.MatchDelimiter(d) {
		return l.nextToken()
	} else {
		return BadDelimiterError{d}
	}
}

func (l *Lexer) EatIntConstant() (types.Int, error) {
	if l.MatchIntConstant() {
		value := l.streamTokenizer.CurrentToken().ValueInt64()
		err := l.nextToken()
		if err != nil {
			return 0, err
		}
		return types.Int(value), nil
	} else {
		return 0, BadIntConstantError{l.streamTokenizer.CurrentToken().ValueString()}
	}
}

func (l *Lexer) EatStrConstant() (string, error) {
	if l.MatchStrConstant() {
		value := l.streamTokenizer.CurrentToken().ValueString()
		err := l.nextToken()
		if err != nil {
			return "", err
		}
		return value, nil
	} else {
		return "", BadStrConstantError{l.streamTokenizer.CurrentToken().ValueString()}
	}
}

func (l *Lexer) EatKeyword(keyword string) error {
	if l.MatchKeyword(keyword) {
		return l.nextToken()
	} else {
		return BadKeywordError{keyword}
	}
}

func (l *Lexer) EatIdentifier() (string, error) {
	if l.MatchIdentifier() {
		value := l.streamTokenizer.CurrentToken().ValueString()
		err := l.nextToken()
		if err != nil {
			return "", err
		}
		return value, nil
	} else {
		return "", BadIdentifierError{l.streamTokenizer.CurrentToken().ValueString()}
	}
}

func (l *Lexer) nextToken() error {
	if l.streamTokenizer.IsValid() {
		l.streamTokenizer.GoNext()
		return nil
	} else {
		return InputStreamReacedEndError{}
	}
}
