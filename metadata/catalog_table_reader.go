package metadata

import (
	"fmt"
	"simple-db-go/record"
	"simple-db-go/transaction"
	"simple-db-go/types"
)

// table_catalog テーブルの１行だけ読み取る.
// table_catalog テーブルのスキーマは固定であるため、TableScan のメソッドではエラーは起こらない. 単に panic とする.
func ReadTableCatalogRow(tableScan *record.TableScan) TableCatalogRow {
	tableName, err := tableScan.GetString("table_name")
	if err != nil {
		panic(fmt.Sprintf("[ReadTableCatalogRow] table_catalog テーブルの table_name 列の読み取りに失敗しました. err=%+v", err))
	}

	slotSize, err := tableScan.GetInt("slot_size")
	if err != nil {
		panic(fmt.Sprintf("[ReadTableCatalogRow] table_catalog テーブルの slot_size 列の読み取りに失敗しました. err=%+v", err))
	}

	return TableCatalogRow{
		TableName: types.TableName(tableName),
		SlotSize:  types.SlotSize(slotSize),
	}
}

// 指定した tableName の行だけ読み取る.
// table_catalog テーブルのスキーマは固定であるため、TableScan のメソッドではエラーは起こらない. 単に panic とする.
func ReadTableCatalogRowFor(tableName types.TableName, transaction *transaction.Transaction, tableManager *TableManager) (TableCatalogRow, error) {
	tableScan := record.NewTableScan(transaction, TABLE_CATALOG_TABLE_NAME, tableManager.tableCatalogLayout)
	defer tableScan.Close()

	for tableScan.Next() {
		gotTableName, err := tableScan.GetString("table_name")
		if err != nil {
			panic(fmt.Sprintf("[ReadTableCatalogRowFor] field_catalog テーブルの table_name 列の読み取りに失敗しました. err=%+v", err))
		}

		gotSlotSize, err := tableScan.GetInt("slot_size")
		if err != nil {
			panic(fmt.Sprintf("[ReadTableCatalogRowFor] field_catalog テーブルの slot_size 列の読み取りに失敗しました. err=%+v", err))
		}

		if gotTableName == string(tableName) {
			return TableCatalogRow{
				TableName: tableName,
				SlotSize:  types.SlotSize(gotSlotSize),
			}, nil
		}
	}

	return TableCatalogRow{}, TableCatalogNotFoundError{TableName: tableName}
}

func ReadFieldCatalogRow(tableScan *record.TableScan) FieldCatalogRow {
	tableName, err := tableScan.GetString("table_name")
	if err != nil {
		panic(fmt.Sprintf("[ReadFieldCatalogRow] field_catalog テーブルの table_name 列の読み取りに失敗しました. err=%+v", err))
	}

	fieldName, err := tableScan.GetString("field_name")
	if err != nil {
		panic(fmt.Sprintf("[ReadFieldCatalogRow] field_catalog テーブルの field_name 列の読み取りに失敗しました. err=%+v", err))
	}

	fieldType, err := tableScan.GetInt("type")
	if err != nil {
		panic(fmt.Sprintf("[ReadFieldCatalogRow] field_catalog テーブルの type 列の読み取りに失敗しました. err=%+v", err))
	}

	fieldLength, err := tableScan.GetInt("length")
	if err != nil {
		panic(fmt.Sprintf("[ReadFieldCatalogRow] field_catalog テーブルの length 列の読み取りに失敗しました. err=%+v", err))
	}

	fieldOffset, err := tableScan.GetInt("offset")
	if err != nil {
		panic(fmt.Sprintf("[ReadFieldCatalogRow] field_catalog テーブルの offset 列の読み取りに失敗しました. err=%+v", err))
	}

	return FieldCatalogRow{
		TableName: types.TableName(tableName),
		FieldName: types.FieldName(fieldName),
		Type:      types.FieldType(fieldType),
		Length:    types.FieldLength(fieldLength),
		Offset:    types.FieldOffsetInSlot(fieldOffset),
	}
}

// 指定したテーブルの field_catalog テーブルの行だけ読み取る.
// field_catalog テーブルのスキーマは固定であるため、TableScan のメソッドではエラーは起こらない. 単に panic とする.
func ReadFieldCatalogRowsFor(tableName types.TableName, transaction *transaction.Transaction, tableManager *TableManager) []FieldCatalogRow {
	fieldCatalogTableScan := record.NewTableScan(transaction, FIELD_CATALOG_TABLE_NAME, tableManager.fieldCatalogLayout)
	defer fieldCatalogTableScan.Close()

	rows := []FieldCatalogRow{}
	for fieldCatalogTableScan.Next() {
		gotTableName, err := fieldCatalogTableScan.GetString("table_name")
		if err != nil {
			panic(fmt.Sprintf("[ReadFieldCatalogRowsFor] field_catalog テーブルの table_name 列の読み取りに失敗しました. err=%+v", err))
		}

		if gotTableName == string(tableName) {
			fieldName, err := fieldCatalogTableScan.GetString("field_name")
			if err != nil {
				panic(fmt.Sprintf("[ReadFieldCatalogRowsFor] field_catalog テーブルの field_name 列の読み取りに失敗しました. err=%+v", err))
			}

			fieldType, err := fieldCatalogTableScan.GetInt("type")
			if err != nil {
				panic(fmt.Sprintf("[ReadFieldCatalogRowsFor] field_catalog テーブルの type 列の読み取りに失敗しました. err=%+v", err))
			}

			fieldLength, err := fieldCatalogTableScan.GetInt("length")
			if err != nil {
				panic(fmt.Sprintf("[ReadFieldCatalogRowsFor] field_catalog テーブルの length 列の読み取りに失敗しました. err=%+v", err))
			}

			fieldOffset, err := fieldCatalogTableScan.GetInt("offset")
			if err != nil {
				panic(fmt.Sprintf("[ReadFieldCatalogRowsFor] field_catalog テーブルの offset 列の読み取りに失敗しました. err=%+v", err))
			}

			rows = append(rows, FieldCatalogRow{
				TableName: tableName,
				FieldName: types.FieldName(fieldName),
				Type:      types.FieldType(fieldType),
				Length:    types.FieldLength(fieldLength),
				Offset:    types.FieldOffsetInSlot(fieldOffset),
			})
		}
	}

	return rows
}

// view_catalog テーブルの１行だけ読み取る.
// view_catalog テーブルのスキーマは固定であるため、TableScan のメソッドではエラーは起こらない. 単に panic とする.
func ReadViewCatalogRow(tableScan *record.TableScan) ViewCatalogRow {
	viewName, err := tableScan.GetString("view_name")
	if err != nil {
		panic(fmt.Sprintf("[ReadViewCatalogRow] view_catalog テーブルの view_name 列の読み取りに失敗しました. err=%+v", err))
	}

	viewDef, err := tableScan.GetString("view_def")
	if err != nil {
		panic(fmt.Sprintf("[ReadViewCatalogRow] view_catalog テーブルの view_def 列の読み取りに失敗しました. err=%+v", err))
	}

	return ViewCatalogRow{
		ViewName: types.ViewName(viewName),
		ViewDef:  types.ViewDef(viewDef),
	}
}

func ReadIndexCatalogRow(tableScan *record.TableScan) IndexCatalogRow {
	indexName, err := tableScan.GetString("index_name")
	if err != nil {
		panic(fmt.Sprintf("[ReadIndexCatalogRow] index_catalog テーブルの index_name 列の読み取りに失敗しました. err=%+v", err))
	}

	tableName, err := tableScan.GetString("table_name")
	if err != nil {
		panic(fmt.Sprintf("[ReadIndexCatalogRow] index_catalog テーブルの table_name 列の読み取りに失敗しました. err=%+v", err))
	}

	fieldName, err := tableScan.GetString("field_name")
	if err != nil {
		panic(fmt.Sprintf("[ReadIndexCatalogRow] index_catalog テーブルの field_name 列の読み取りに失敗しました. err=%+v", err))
	}

	return IndexCatalogRow{
		IndexName: types.IndexName(indexName),
		TableName: types.TableName(tableName),
		FieldName: types.FieldName(fieldName),
	}
}
