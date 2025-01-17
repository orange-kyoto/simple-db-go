package parsing

import "strings"

var simpleDBKeywords = map[string]struct{}{
	"SELECT":  {},
	"FROM":    {},
	"WHERE":   {},
	"AND":     {},
	"INSERT":  {},
	"INTO":    {},
	"VALUES":  {},
	"DELETE":  {},
	"UPDATE":  {},
	"SET":     {},
	"CREATE":  {},
	"TABLE":   {},
	"INT":     {},
	"VARCHAR": {},
	"VIEW":    {},
	"AS":      {},
	"INDEX":   {},
	"ON":      {},
}

var simpleDBKeywordList = make([]string, 0, len(simpleDBKeywords))

func init() {
	simpleDBKeywordList = make([]string, 0, len(simpleDBKeywords))
	for k := range simpleDBKeywords {
		simpleDBKeywordList = append(simpleDBKeywordList, k)
	}
}

func IsSimpleDBKeyword(s string) bool {
	_, exists := simpleDBKeywords[strings.ToUpper(s)]
	return exists
}
