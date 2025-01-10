package metadata

import (
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

	// table_catalog というテーブルに、テーブルカタログの情報を登録している.
	// 1行だけ登録.
	tableCatalogTableScan := record.NewTableScan(transaction, TABLE_CATALOG_TABLE_NAME, tm.tableCatalogLayout)
	tableCatalogTableScan.Insert()
	tableCatalogTableScan.SetString("table_name", string(tableName))
	tableCatalogTableScan.SetInt("slot_size", layout.GetSlotSize())
	tableCatalogTableScan.Close()

	// field_catalog というテーブルに、各フィールドのメタデータを登録している.
	// こちらはフィールド数分登録.
	fieldCatalogTableScan := record.NewTableScan(transaction, FIELD_CATALOG_TABLE_NAME, tm.fieldCatalogLayout)
	for _, fieldName := range schema.Fields() {
		fieldCatalogTableScan.Insert()
		fieldCatalogTableScan.SetString("table_name", string(tableName))
		fieldCatalogTableScan.SetString("field_name", string(fieldName))
		fieldCatalogTableScan.SetInt("type", types.Int(schema.FieldType(fieldName)))
		fieldCatalogTableScan.SetInt("length", types.Int(schema.Length(fieldName)))
		fieldCatalogTableScan.SetInt("offset", types.Int(layout.GetOffset(fieldName)))
	}
	fieldCatalogTableScan.Close()
}

func (tm *TableManager) GetLayout(tableName types.TableName, transaction *transaction.Transaction) (*record.Layout, error) {
	tableCatalogTableScan := record.NewTableScan(transaction, TABLE_CATALOG_TABLE_NAME, tm.tableCatalogLayout)

	slotSize := types.Int(-1)
	for tableCatalogTableScan.Next() {
		if tableCatalogTableScan.GetString("table_name") == string(tableName) {
			slotSize = tableCatalogTableScan.GetInt("slot_size")
			break
		}
	}
	tableCatalogTableScan.Close()

	if slotSize == types.Int(-1) {
		return nil, TableCatalogNotFoundError{TableName: tableName}
	}

	schema := record.NewSchema()
	offsets := make(map[types.FieldName]types.FieldOffsetInSlot)
	fieldCatalogTableScan := record.NewTableScan(transaction, FIELD_CATALOG_TABLE_NAME, tm.fieldCatalogLayout)

	for fieldCatalogTableScan.Next() {
		if fieldCatalogTableScan.GetString("table_name") == string(tableName) {
			fieldName := types.FieldName(fieldCatalogTableScan.GetString("field_name"))
			fieldType := types.FieldType(fieldCatalogTableScan.GetInt("type"))
			fieldLength := types.FieldLength(fieldCatalogTableScan.GetInt("length"))
			fieldOffset := types.FieldOffsetInSlot(fieldCatalogTableScan.GetInt("offset"))
			offsets[fieldName] = fieldOffset
			schema.AddField(fieldName, fieldType, fieldLength)
		}
	}
	fieldCatalogTableScan.Close()

	if len(schema.Fields()) == 0 {
		return nil, FieldCatalogNotFoundError{TableName: tableName}
	}

	return record.NewLayoutWith(schema, offsets, slotSize), nil
}
