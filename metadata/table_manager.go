package metadata

import (
	"fmt"
	"simple-db-go/constants"
	"simple-db-go/record"
	"simple-db-go/transaction"
	"simple-db-go/types"
)

type TableManager struct {
	// テーブルのカタログ(metadata)の物理情報
	tableCatalogLayout *record.Layout
	// フィールドのカタログ(metadata)の物理情報
	fieldCatalogLayout *record.Layout
}

// NOTE: システム起動中に一度だけ呼ばれる.
func NewTableManager(isNew bool, transaction *transaction.Transaction) *TableManager {
	tableCatalogSchema := record.NewSchema()
	tableCatalogSchema.AddStringField("table_name", constants.MAX_NAME_LENGTH)
	tableCatalogSchema.AddIntField("slot_size")
	tableCatalogLayout := record.NewLayout(tableCatalogSchema)

	fieldCatalogSchema := record.NewSchema()
	fieldCatalogSchema.AddStringField("table_name", constants.MAX_NAME_LENGTH)
	fieldCatalogSchema.AddStringField("field_name", constants.MAX_NAME_LENGTH)
	fieldCatalogSchema.AddIntField("type")
	fieldCatalogSchema.AddIntField("length")
	fieldCatalogSchema.AddIntField("offset")
	fieldCatalogLayout := record.NewLayout(fieldCatalogSchema)

	tableManager := &TableManager{
		tableCatalogLayout: tableCatalogLayout,
		fieldCatalogLayout: fieldCatalogLayout,
	}

	if isNew {
		tableManager.CreateTable(TABLE_CATALOG_TABLE_NAME, tableCatalogSchema, transaction)
		tableManager.CreateTable(FIELD_CATALOG_TABLE_NAME, fieldCatalogSchema, transaction)
	}

	return tableManager
}

// schema, layout の情報をもとに、カタログレコードを登録する.
// TableScan を利用してカタログレコードを登録する.
func (tm *TableManager) CreateTable(tableName types.TableName, schema *record.Schema, transaction *transaction.Transaction) {
	layout := record.NewLayout(schema)

	WriteTableCatalogRow(transaction, tm, TableCatalogRow{TableName: tableName, SlotSize: layout.GetSlotSize()})

	rows := []FieldCatalogRow{}
	for _, fieldName := range schema.Fields() {
		// schema 自身から生成したフィールドの値を取得しているのでエラーは発生し得ない。単に panic する.
		fieldType, err := schema.FieldType(fieldName)
		if err != nil {
			panic(fmt.Sprintf("TableManager.CreateTable で予期せぬエラーが発生しました. schema=%+v, fieldName=%+v, err=%+v", schema, fieldName, err))
		}

		fieldLength, err := schema.Length(fieldName)
		if err != nil {
			panic(fmt.Sprintf("TableManager.CreateTable で予期せぬエラーが発生しました. schema=%+v, fieldName=%+v, err=%+v", schema, fieldName, err))
		}

		fieldOffset, err := layout.GetOffset(fieldName)
		if err != nil {
			panic(fmt.Sprintf("TableManager.CreateTable で予期せぬエラーが発生しました. layout=%+v, fieldName=%+v, err=%+v", layout, fieldName, err))
		}

		rows = append(rows, FieldCatalogRow{TableName: tableName, FieldName: fieldName, Type: fieldType, Length: fieldLength, Offset: fieldOffset})
	}
	WriteFieldCatalogRows(transaction, tm, rows)
}

func (tm *TableManager) GetLayout(tableName types.TableName, transaction *transaction.Transaction) (*record.Layout, error) {

	tableCatalogRow, err := ReadTableCatalogRowFor(tableName, transaction, tm)
	if err != nil {
		return nil, err
	}

	schema := record.NewSchema()
	offsets := make(map[types.FieldName]types.FieldOffsetInSlot)

	fieldCatalogRows := ReadFieldCatalogRowsFor(tableName, transaction, tm)
	if len(fieldCatalogRows) == 0 {
		return nil, FieldCatalogNotFoundError{TableName: tableName}
	}

	for _, row := range fieldCatalogRows {
		offsets[row.FieldName] = row.Offset
		schema.AddField(row.FieldName, row.Type, row.Length)
	}

	return record.NewLayoutWith(schema, offsets, tableCatalogRow.SlotSize), nil
}

func (tm *TableManager) GetTableCatalogLayout() *record.Layout {
	return tm.tableCatalogLayout
}

func (tm *TableManager) GetFieldCatalogLayout() *record.Layout {
	return tm.fieldCatalogLayout
}
