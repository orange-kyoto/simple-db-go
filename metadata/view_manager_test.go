package metadata

import (
	"path"
	"simple-db-go/constants"
	"simple-db-go/record"
	"simple-db-go/test_util"
	"simple-db-go/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestViewManagerNewViewManager(t *testing.T) {
	t.Run("isNew = false で呼ばれた場合はカタログテーブルは作られない.", func(t *testing.T) {
		transaction := test_util.StartNewTransaction(viewManagerTestName)
		defer transaction.Rollback()

		tableManager := NewTableManager(false, transaction)
		NewViewManager(false, tableManager, transaction)

		assert.NoFileExists(t, path.Join(viewManagerTestName, "view_catalog.table"), "view_catalog.table が存在してはいけない.")
	})

	t.Run("isNew = true で呼ばれた場合はビューカタログテーブルが作られ、期待したレコードが登録されている.", func(t *testing.T) {
		transaction := test_util.StartNewTransaction(viewManagerTestName)
		defer transaction.Rollback()

		tableManager := NewTableManager(true, transaction)
		NewViewManager(true, tableManager, transaction)

		tableCatalogLayout, _ := tableManager.GetLayout(TABLE_CATALOG_TABLE_NAME, transaction)
		fieldCatalogLayout, _ := tableManager.GetLayout(FIELD_CATALOG_TABLE_NAME, transaction)
		viewCatalogLayout, _ := tableManager.GetLayout(VIEW_CATALOG_TABLE_NAME, transaction)
		tableCatalogTableScan := record.NewTableScan(transaction, TABLE_CATALOG_TABLE_NAME, tableCatalogLayout)
		fieldCatalogTableScan := record.NewTableScan(transaction, FIELD_CATALOG_TABLE_NAME, fieldCatalogLayout)
		viewCatalogTableScan := record.NewTableScan(transaction, VIEW_CATALOG_TABLE_NAME, viewCatalogLayout)

		t.Run("テーブルカタログにビューカタログのレコードが登録されている.", func(t *testing.T) {
			expectedRecords := []TableCatalogRow{
				{VIEW_CATALOG_TABLE_NAME, 128},
			}
			actualRecords := make([]TableCatalogRow, 0)
			for tableCatalogTableScan.Next() {
				actualRecords = append(actualRecords, TableCatalogRow{
					TableName: types.TableName(tableCatalogTableScan.GetString("table_name")),
					SlotSize:  types.SlotSize(tableCatalogTableScan.GetInt("slot_size")),
				})
			}
			assert.Subset(t, actualRecords, expectedRecords, "テーブルカタログにビューカタログのレコードが登録されているはず.")
		})

		t.Run("フィールドカタログにビューカタログのレコードが登録されている.", func(t *testing.T) {
			expectedRecords := []FieldCatalogRow{
				{VIEW_CATALOG_TABLE_NAME, "view_name", constants.VARCHAR, 16, 4},
				{VIEW_CATALOG_TABLE_NAME, "view_def", constants.VARCHAR, 100, 24},
			}
			actualRecords := make([]FieldCatalogRow, 0, 2)
			for fieldCatalogTableScan.Next() {
				actualRecords = append(actualRecords, FieldCatalogRow{
					TableName: types.TableName(fieldCatalogTableScan.GetString("table_name")),
					FieldName: types.FieldName(fieldCatalogTableScan.GetString("field_name")),
					Type:      types.FieldType(fieldCatalogTableScan.GetInt("type")),
					Length:    types.FieldLength(fieldCatalogTableScan.GetInt("length")),
					Offset:    types.FieldOffsetInSlot(fieldCatalogTableScan.GetInt("offset")),
				})
			}
			assert.Subset(t, actualRecords, expectedRecords, "フィールドカタログにビューカタログのレコードが登録されているはず.")
		})

		t.Run("ビューは作成していないので、ビューカタログはレコードが０件である.", func(t *testing.T) {
			assert.False(t, viewCatalogTableScan.Next(), "ビューカタログにはレコードが登録されていないはず.")
		})
	})
}

func TestViewManagerGetCreateView(t *testing.T) {
	transaction := test_util.StartNewTransaction(viewManagerTestName)
	defer transaction.Rollback()
	tableManager := NewTableManager(true, transaction)
	viewManager := NewViewManager(true, tableManager, transaction)

	t.Run("正常にビューの作成と取得が行える.", func(t *testing.T) {
		testViewName := types.ViewName("test_view")
		testViewDef := types.ViewDef("SELECT * FROM test_table;")

		t.Run("正常にビューの作成ができる.", func(t *testing.T) {
			assert.NotPanics(t, func() { viewManager.CreateView(testViewName, testViewDef, transaction) }, "ビューの作成に失敗してはいけない.")

			viewCatalogLayout, _ := tableManager.GetLayout(VIEW_CATALOG_TABLE_NAME, transaction)
			viewCatalogTableScan := record.NewTableScan(transaction, VIEW_CATALOG_TABLE_NAME, viewCatalogLayout)

			assert.True(t, viewCatalogTableScan.Next(), "ビューカタログにレコードが登録されているはず.")
			assert.Equal(t, string(testViewName), viewCatalogTableScan.GetString("view_name"), "ビューカタログの view_name が期待した値であるはず.")
			assert.Equal(t, string(testViewDef), viewCatalogTableScan.GetString("view_def"), "ビューカタログの view_def が期待した値であるはず.")
			assert.False(t, viewCatalogTableScan.Next(), "ビューカタログには１行しか登録されていないはず.")
		})

		t.Run("先ほど作成したビューの取得ができる.", func(t *testing.T) {
			actualViewDef, err := viewManager.GetView(testViewName, transaction)
			if assert.NoError(t, err, "ビューの取得に失敗してはいけない.") {
				assert.Equal(t, testViewDef, actualViewDef, "ビューの定義が期待した値であるはず.")
			}
		})

		t.Run("作成していないビューの取得をするとエラーが返る.", func(t *testing.T) {
			actualViewDef, err := viewManager.GetView("hoge_view", transaction)
			assert.Error(t, err, "存在しないビューの取得ではエラーを返すべし.")
			assert.IsType(t, CannotGetViewError{}, err, "存在しないビューの取得では CannotGetViewError を返すべし.")
			assert.Empty(t, actualViewDef, "存在しないビューの取得では空文字を返すべし.")
		})
	})
}
