package metadata

import "simple-db-go/types"

// テーブルカタログを記録するテーブル名.
const TABLE_CATALOG_TABLE_NAME = "table_catalog"

// テーブルカタログテーブルの１行を表す.
type TableCatalogRow struct {
	TableName types.TableName
	SlotSize  types.SlotSize
}

// フィールドカタログを記録するテーブル名.
const FIELD_CATALOG_TABLE_NAME = "field_catalog"

// フィールドカタログテーブルの１行を表す.
type FieldCatalogRow struct {
	TableName types.TableName
	FieldName types.FieldName
	Type      types.FieldType
	Length    types.FieldLength
	Offset    types.FieldOffsetInSlot
}

// ビューカタログを記録するテーブル名.
const VIEW_CATALOG_TABLE_NAME = "view_catalog"

type ViewCatalogRow struct {
	ViewName types.ViewName
	ViewDef  types.ViewDef
}

// インデックスカタログを記録するテーブル名.
const INDEX_CATALOG_TABLE_NAME = "index_catalog"

type IndexCatalogRow struct {
	IndexName types.IndexName
	TableName types.TableName
	FieldName types.FieldName
}
