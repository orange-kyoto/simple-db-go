package metadata

import (
	"os"
	"path"
	"simple-db-go/record"
	"simple-db-go/test_util"
	"simple-db-go/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableManagerInitialization(t *testing.T) {
	transaction := test_util.StartNewTransaction(tableManagerTestName)
	defer transaction.Rollback()

	t.Run("isNew = false で呼ばれた場合はカタログテーブルは作られない.", func(t *testing.T) {
		NewTableManager(false, transaction)

		_, err := os.Stat(path.Join(tableManagerTestName, "table_catalog.table"))
		assert.True(t, os.IsNotExist(err), "table_catalog.table が存在してはいけない.")

		_, err = os.Stat(path.Join(tableManagerTestName, "field_catalog.table"))
		assert.True(t, os.IsNotExist(err), "field_catalog.table が存在してはいけない.")
	})

	tableManager := NewTableManager(true, transaction)

	t.Run("isNew = true で呼ばれた場合はテーブルカタログテーブルが作られ、期待したレコードが登録されている.", func(t *testing.T) {
		tableCatalogTableScan := record.NewTableScan(transaction, TABLE_CATALOG_TABLE_NAME, tableManager.tableCatalogLayout)

		assert.True(t, tableCatalogTableScan.HasField("table_name"), "table_name フィールドが存在しているはず.")
		assert.True(t, tableCatalogTableScan.HasField("slot_size"), "slot_size フィールドが存在しているはず.")

		// table_catalog には２行登録されているはず。
		tests := []struct {
			tableName string
			slotSize  types.Int
		}{
			// table_name: 4+16, slot_size: 4, flag: 4
			{TABLE_CATALOG_TABLE_NAME, 28},
			// table_name: 4+16, field_name: 4+16, type: 4, length: 4, offset: 4, flag: 4
			{FIELD_CATALOG_TABLE_NAME, 56},
		}

		for _, test := range tests {
			exists := tableCatalogTableScan.Next()
			assert.Truef(t, exists, "テーブルカタログにレコードが登録されているはず. table_name=%s\n", test.tableName)
			assert.Equalf(t, test.tableName, tableCatalogTableScan.GetString("table_name"), "table_name が期待した値であるはず. table_name=%s\n", test.tableName)
			assert.Equalf(t, test.slotSize, tableCatalogTableScan.GetInt("slot_size"), "slot_size が期待した値であるはず. table_name=%s\n", test.tableName)
		}

		assert.False(t, tableCatalogTableScan.Next(), "テーブルカタログには２行しか登録されていないはず.")
	})

	t.Run("isNew = true で呼ばれた場合はフィールドカタログテーブルが作られ、期待したレコードが登録されている.", func(t *testing.T) {
		fieldCatalogTableScan := record.NewTableScan(transaction, FIELD_CATALOG_TABLE_NAME, tableManager.fieldCatalogLayout)

		assert.True(t, fieldCatalogTableScan.HasField("table_name"), "table_name フィールドが存在しているはず.")
		assert.True(t, fieldCatalogTableScan.HasField("field_name"), "field_name フィールドが存在しているはず.")
		assert.True(t, fieldCatalogTableScan.HasField("type"), "type フィールドが存在しているはず.")
		assert.True(t, fieldCatalogTableScan.HasField("length"), "length フィールドが存在しているはず.")
		assert.True(t, fieldCatalogTableScan.HasField("offset"), "offset フィールドが存在しているはず.")

		tests := []struct {
			tableName string
			fieldName record.FieldName
			fieldType record.FieldType
			length    record.FieldLength
			offset    record.FieldOffsetInSlot
		}{
			// 注意：INTEGER フィールドは固定長であり、length は使わないので全て0としている.
			// table_catalog テーブルのフィールド情報
			{TABLE_CATALOG_TABLE_NAME, "table_name", record.VARCHAR, 16, 4},
			{TABLE_CATALOG_TABLE_NAME, "slot_size", record.INTEGER, 0, 24},
			// field_catalog テーブルのフィールド情報
			{FIELD_CATALOG_TABLE_NAME, "table_name", record.VARCHAR, 16, 4},
			{FIELD_CATALOG_TABLE_NAME, "field_name", record.VARCHAR, 16, 24},
			{FIELD_CATALOG_TABLE_NAME, "type", record.INTEGER, 0, 44},
			{FIELD_CATALOG_TABLE_NAME, "length", record.INTEGER, 0, 48},
			{FIELD_CATALOG_TABLE_NAME, "offset", record.INTEGER, 0, 52},
		}

		for _, test := range tests {
			exists := fieldCatalogTableScan.Next()
			assert.Truef(t, exists, "フィールドカタログにレコードが登録されているはず. table_name=%s, field_name=%s\n", test.tableName, test.fieldName)
			assert.Equalf(t, test.tableName, fieldCatalogTableScan.GetString("table_name"), "table_name が期待した値であるはず. table_name=%s, field_name=%s\n", test.tableName, test.fieldName)
			assert.Equalf(t, test.fieldName, record.FieldName(fieldCatalogTableScan.GetString("field_name")), "field_name が期待した値であるはず. table_name=%s, field_name=%s\n", test.tableName, test.fieldName)
			assert.Equalf(t, test.fieldType, record.FieldType(fieldCatalogTableScan.GetInt("type")), "type が期待した値であるはず. table_name=%s, field_name=%s\n", test.tableName, test.fieldName)
			assert.Equalf(t, test.length, record.FieldLength(fieldCatalogTableScan.GetInt("length")), "length が期待した値であるはず. table_name=%s, field_name=%s\n", test.tableName, test.fieldName)
			assert.Equalf(t, test.offset, record.FieldOffsetInSlot(fieldCatalogTableScan.GetInt("offset")), "offset が期待した値であるはず. table_name=%s, field_name=%s\n", test.tableName, test.fieldName)
		}

		assert.False(t, fieldCatalogTableScan.Next(), "フィールドカタログには7行しか登録されていないはず.")
	})
}

func TestTableManagerCreateTable(t *testing.T) {
	t.Skip("NewTableManager のテストで2つのカタログテーブルに対する CreateTable を検証しているので、ここではスキップする.")
}

func TestTableManagerGetLayout(t *testing.T) {
	transaction := test_util.StartNewTransaction(tableManagerTestName)
	defer transaction.Rollback()
	tableManager := NewTableManager(true, transaction)

	// 検証用のスキーマを用意する.
	testSchema := record.NewSchema()
	testSchema.AddIntField("id")
	testSchema.AddStringField("name", 10)
	testSchema.AddStringField("address", 20)
	testSchema.AddIntField("age")
	testLayout := record.NewLayout(testSchema)
	tableManager.CreateTable("test_table", testSchema, transaction)

	t.Run("各テーブルのレイアウトを正しく取得できる", func(t *testing.T) {
		tests := []struct {
			tableName string
			layout    *record.Layout
		}{
			{TABLE_CATALOG_TABLE_NAME, tableManager.tableCatalogLayout},
			{FIELD_CATALOG_TABLE_NAME, tableManager.fieldCatalogLayout},
			{"test_table", testLayout},
		}

		for _, test := range tests {
			layout, err := tableManager.GetLayout(test.tableName, transaction)
			if assert.NoErrorf(t, err, "存在するテーブルと期待されるので、GetLayout はエラーを返してはいけない. table_name=%s\n", test.tableName) {
				assert.NotNilf(t, layout, "存在するテーブルと期待されるので、GetLayout は nil を返してはいけない. table_name=%s\n", test.tableName)
				assert.Equalf(t, test.layout, layout, "GetLayout は期待される Layout を返すべし. table_name=%s\n", test.tableName)
				assert.NotSamef(t, test.layout, layout, "GetLayout はDBテーブルから新たに Layout を復元するので、ポインタは異なっているべし. table_name=%s\n", test.tableName)
			}
		}
	})

	t.Run("存在しないテーブルのレイアウトを取得しようとするとエラーを返す.", func(t *testing.T) {
		layout, err := tableManager.GetLayout("not_exist_table", transaction)
		if assert.Error(t, err, "存在しないテーブルのレイアウトを取得しようとするとエラーを返すべし.") {
			assert.Nil(t, layout, "存在しないテーブルのレイアウトを取得しようとすると nil を返すべし.")
			assert.IsType(t, TableCatalogNotFoundError{}, err, "エラーは TableCatalogNotFoundError であるべし.")
		}
	})
}
