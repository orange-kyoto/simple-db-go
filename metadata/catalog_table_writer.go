package metadata

import (
	"fmt"
	"simple-db-go/record"
	"simple-db-go/transaction"
	"simple-db-go/types"
)

// table_catalog テーブルのスキーマは固定であるため、TableScan.SetString,SetInt のエラーは起こり得ない.
// 単に panic させる.
func WriteTableCatalogRow(transaction *transaction.Transaction, tableManager *TableManager, row TableCatalogRow) {
	tableCatalogTableScan := record.NewTableScan(transaction, TABLE_CATALOG_TABLE_NAME, tableManager.tableCatalogLayout)
	defer tableCatalogTableScan.Close()

	tableCatalogTableScan.Insert()

	err := tableCatalogTableScan.SetString("table_name", string(row.TableName))
	if err != nil {
		panic(fmt.Sprintf("[WriteTableCatalogRow] table_catalog テーブルの table_name に文字列をセットできませんでした. row=%+v, error=%+v", row, err))
	}

	err = tableCatalogTableScan.SetInt("slot_size", types.Int(row.SlotSize))
	if err != nil {
		panic(fmt.Sprintf("[WriteTableCatalogRow] table_catalog テーブルの slot_size に整数をセットできませんでした. row=%+v, error=%+v", row, err))
	}
}

// field_catalog テーブルのスキーマは固定であるため、TableScan.SetString,SetInt のエラーは起こり得ない.
// 単に panic させる.
func WriteFieldCatalogRows(transaction *transaction.Transaction, tableManager *TableManager, rows []FieldCatalogRow) {
	fieldCatalogTableScan := record.NewTableScan(transaction, FIELD_CATALOG_TABLE_NAME, tableManager.fieldCatalogLayout)
	defer fieldCatalogTableScan.Close()

	for _, row := range rows {
		fieldCatalogTableScan.Insert()

		err := fieldCatalogTableScan.SetString("table_name", string(row.TableName))
		if err != nil {
			panic(fmt.Sprintf("[WriteFieldCatalogRow] field_catalog テーブルの table_name に文字列をセットできませんでした. row=%+v, error=%+v", row, err))
		}

		err = fieldCatalogTableScan.SetString("field_name", string(row.FieldName))
		if err != nil {
			panic(fmt.Sprintf("[WriteFieldCatalogRow] field_catalog テーブルの field_name に文字列をセットできませんでした. row=%+v, error=%+v", row, err))
		}

		err = fieldCatalogTableScan.SetInt("type", types.Int(row.Type))
		if err != nil {
			panic(fmt.Sprintf("[WriteFieldCatalogRow] field_catalog テーブルの type に整数をセットできませんでした. row=%+v, error=%+v", row, err))
		}

		err = fieldCatalogTableScan.SetInt("length", types.Int(row.Length))
		if err != nil {
			panic(fmt.Sprintf("[WriteFieldCatalogRow] field_catalog テーブルの length に整数をセットできませんでした. row=%+v, error=%+v", row, err))
		}

		err = fieldCatalogTableScan.SetInt("offset", types.Int(row.Offset))
		if err != nil {
			panic(fmt.Sprintf("[WriteFieldCatalogRow] field_catalog テーブルの offset に整数をセットできませんでした. row=%+v, error=%+v", row, err))
		}
	}

}

// view_catalog テーブルのスキーマは固定であるため、TableScan.SetString,SetInt のエラーは起こり得ない.
// 単に panic させる.
func WriteViewCatalogRow(row ViewCatalogRow, transaction *transaction.Transaction, tableManager *TableManager) {
	layout, err := tableManager.GetLayout(VIEW_CATALOG_TABLE_NAME, transaction)
	if err != nil {
		// 初期起動時に必ずカタログのレイアウトが登録されているはずなので、ここは panic にしておく.
		panic(fmt.Sprintf("view_catalog レコードの書き込みに失敗しました. err=%+v", err))
	}

	viewCatalogTableScan := record.NewTableScan(transaction, VIEW_CATALOG_TABLE_NAME, layout)
	defer viewCatalogTableScan.Close()

	viewCatalogTableScan.Insert()

	err = viewCatalogTableScan.SetString("view_name", string(row.ViewName))
	if err != nil {
		panic(fmt.Sprintf("[WriteViewCatalogRow] view_catalog テーブルの view_name に文字列をセットできませんでした. row=%+v, error=%+v", row, err))
	}

	err = viewCatalogTableScan.SetString("view_def", string(row.ViewDef))
	if err != nil {
		panic(fmt.Sprintf("[WriteViewCatalogRow] view_catalog テーブルの view_def に文字列をセットできませんでした. row=%+v, error=%+v", row, err))
	}
}
