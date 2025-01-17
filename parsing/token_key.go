package parsing

import (
	"fmt"

	"github.com/bzick/tokenizer"
)

const (
	Delimiter tokenizer.TokenKey = iota + 1 // commna(,) or equal(=), etc
	StrConstant
	Keyword
)

func TokenKeyToString(key tokenizer.TokenKey) string {
	switch key {
	case Delimiter:
		return "Delimiter"
	case StrConstant:
		return "StrConstant"
	case Keyword:
		return "Keyword"
	case tokenizer.TokenKeyword:
		return "tokenizer.TokenKeyword"
	case tokenizer.TokenInteger:
		return "tokenizer.TokenInteger"
	default:
		return fmt.Sprintf("%+v", key)
	}
}
