package util

import (
	"os"
	"simple-db-go/types"
)

// types.Int 型として長さを返す len() のラッパー関数
func Len[T ~string | ~[]byte](slice T) types.Int {
	return types.Int(len(slice))
}

func Cleanup(path string) {
	os.RemoveAll(path)
}
