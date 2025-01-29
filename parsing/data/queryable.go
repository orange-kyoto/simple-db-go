package data

import "simple-db-go/types"

// SELECT 句においては、FROM の後にテーブル名もしくはビュー名を指定できる.
// パース時点では区別がつかないので、この識別子を扱うための構造体として用意する.
type Queryable string

func (q Queryable) ToTableName() types.TableName {
	return types.TableName(q)
}

func (q Queryable) ToViewName() types.ViewName {
	return types.ViewName(q)
}

func (q Queryable) ToString() string {
	return string(q)
}
