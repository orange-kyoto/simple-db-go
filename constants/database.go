package constants

// テーブルやカラム名の最大長.
const MAX_NAME_LENGTH = 16

// View の定義本体の最大文字数
// NOTE: もちろん、ありえないくらい小さすぎる。書籍には、clob(9999)とかの方がマシと書いてある.
const MAX_VIEW_DEF_LENGTH = 100
