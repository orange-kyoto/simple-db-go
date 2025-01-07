package record

import "simple-db-go/types"

// DBレコードのフィールド名
type FieldName string

// DBレコードのフィールドの型
type FieldType types.Int

// DBレコードのフィールドの長さ. 文字列フィールドの場合、これは最大文字数であり、バイトサイズではない.
// 整数フィールドの場合は 0 とし、この値は使わない（固定長のため）.
type FieldLength types.Int

// 各スロット内におけるフィールドのオフセット
type FieldOffset types.Int

// jdbc の値に合わせている.
// https://docs.oracle.com/javase/jp/8/docs/api/java/sql/Types.html
const (
	INTEGER FieldType = 4
	VARCHAR FieldType = 12
)

const (
	// 整数フィールドの場合は 0 とし、この値は使わない（固定長のため）.
	INTEGER_FIELD_LENGTH FieldLength = 0
)

type fieldInfo struct {
	fieldType FieldType
	length    FieldLength
}

func newFieldInfo(fieldType FieldType, length FieldLength) fieldInfo {
	return fieldInfo{fieldType: fieldType, length: length}
}
