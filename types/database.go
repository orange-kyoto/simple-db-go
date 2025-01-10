package types

// DB テーブル名
type TableName string

// DBレコードのフィールド名
type FieldName string

// DBレコードのフィールドの型
type FieldType Int

// DBレコードのフィールドの長さ. 文字列フィールドの場合、これは最大文字数であり、バイトサイズではない.
// 整数フィールドの場合は 0 とし、この値は使わない（固定長のため）.
type FieldLength Int

// 各スロット内における、フィールドのオフセット
// 前にあるフィールドの長さの合計＋フラグの長さ(4bytes)
type FieldOffsetInSlot Int

// DB ビュー名
type ViewName string

// DB インデックス名
type IndexName string
