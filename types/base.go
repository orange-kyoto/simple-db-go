package types

import "strconv"

// 基本的に今回のDB実装では int32 で扱うことにする
type Int int32

func (i Int) ToString() string {
	return strconv.Itoa(int(i))
}
