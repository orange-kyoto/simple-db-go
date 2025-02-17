package metadata

import (
	"fmt"
	"simple-db-go/types"
)

type TableCatalogNotFoundError struct {
	TableName types.TableName
}

func (e TableCatalogNotFoundError) Error() string {
	return fmt.Sprintf("[Metadata Error] テーブルカタログが見つかりませんでした. table_name=%s", e.TableName)
}

type FieldCatalogNotFoundError struct {
	TableName types.TableName
}

func (e FieldCatalogNotFoundError) Error() string {
	return fmt.Sprintf("[Metadata Error] フィールドカタログが見つかりませんでした. table_name=%s", e.TableName)
}

type CannotGetViewError struct {
	ViewName types.ViewName
	error    error
}

func (e CannotGetViewError) Error() string {
	return fmt.Sprintf("[Metadata Error] ビューの取得に失敗しました. view_name=%s, error=%+v", e.ViewName, e.error)
}

type CannotGetIndexInfoError struct {
	TableName types.TableName
	error     error
}

func (e CannotGetIndexInfoError) Error() string {
	return fmt.Sprintf("[Metadata Error] インデックス統計情報の取得に失敗しました. table_name=%s, error=%+v", e.TableName, e.error)
}
