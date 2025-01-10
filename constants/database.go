package constants

import "simple-db-go/types"

// テーブルやカラム名の最大長.
const MAX_NAME_LENGTH = 16

// View の定義本体の最大文字数
// NOTE: もちろん、ありえないくらい小さすぎる。書籍には、clob(9999)とかの方がマシと書いてある.
const MAX_VIEW_DEF_LENGTH = 100

// jdbc の値に合わせている.
// https://docs.oracle.com/javase/jp/8/docs/api/java/sql/Types.html
const (
	INTEGER types.FieldType = 4
	VARCHAR types.FieldType = 12
)
