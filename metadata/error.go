package metadata

import "fmt"

type TableCatalogNotFoundError struct {
	TableName string
}

func (e TableCatalogNotFoundError) Error() string {
	return fmt.Sprintf("[Metadata Error] テーブルカタログが見つかりませんでした. table_name=%s", e.TableName)
}

type FieldCatalogNotFoundError struct {
	TableName string
}

func (e FieldCatalogNotFoundError) Error() string {
	return fmt.Sprintf("[Metadata Error] フィールドカタログが見つかりませんでした. table_name=%s", e.TableName)
}
