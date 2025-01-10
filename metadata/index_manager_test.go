package metadata

import (
	"path"
	"simple-db-go/constants"
	"simple-db-go/record"
	"simple-db-go/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexManagerNewIndexManager(t *testing.T) {
	transaction := newTransactionForTest(t, indexManagerTestName)
	defer transaction.Rollback()

	tableManager := NewTableManager(true, transaction)
	statManager := NewStatManager(tableManager, transaction)

	t.Run("インデックスカタログテーブルを初期化していないのに isNew = false で起動すると異常系としてpanicする.", func(t *testing.T) {
		assert.Panics(t, func() { NewIndexManager(false, tableManager, statManager, transaction) })
	})

	t.Run("isNew = true で初期化したとき、インデックスカタログテーブルが作られる.", func(t *testing.T) {
		NewIndexManager(true, tableManager, statManager, transaction)

		t.Run("インデックスカタログテーブルのファイルはまだ作成されていないこと.", func(t *testing.T) {
			assert.NoFileExists(t, path.Join(indexManagerTestName, INDEX_CATALOG_TABLE_NAME+".table"), "まだインデックス自体は作っていないので、ファイルは存在しないはず.")
		})

		t.Run("table_catalog テーブルに期待するレコードが入っていること.", func(t *testing.T) {
			tableCatalogLayout, _ := tableManager.GetLayout(TABLE_CATALOG_TABLE_NAME, transaction)
			tableScan := record.NewTableScan(transaction, TABLE_CATALOG_TABLE_NAME, tableCatalogLayout)
			defer tableScan.Close()

			actualRows := make([]TableCatalogRow, 0)
			for tableScan.Next() {
				actualRows = append(
					actualRows,
					TableCatalogRow{TableName: types.TableName(tableScan.GetString("table_name")), SlotSize: types.SlotSize(tableScan.GetInt("slot_size"))},
				)
			}
			expectedRow := TableCatalogRow{TableName: INDEX_CATALOG_TABLE_NAME, SlotSize: 64}

			assert.Contains(t, actualRows, expectedRow, "table_catalog テーブルに期待するレコードが入っていること.")
		})

		t.Run("field_catalog テーブルに期待するレコードが入っていること.", func(t *testing.T) {
			fieldCatalogLayout, _ := tableManager.GetLayout(FIELD_CATALOG_TABLE_NAME, transaction)
			tableScan := record.NewTableScan(transaction, FIELD_CATALOG_TABLE_NAME, fieldCatalogLayout)
			defer tableScan.Close()

			actualRows := make([]FieldCatalogRow, 0)
			for tableScan.Next() {
				actualRows = append(
					actualRows,
					FieldCatalogRow{
						TableName: types.TableName(tableScan.GetString("table_name")),
						FieldName: types.FieldName(tableScan.GetString("field_name")),
						Type:      types.FieldType(tableScan.GetInt("type")),
						Length:    types.FieldLength(tableScan.GetInt("length")),
						Offset:    types.FieldOffsetInSlot(tableScan.GetInt("offset")),
					},
				)
			}
			expectedRows := []FieldCatalogRow{
				{TableName: INDEX_CATALOG_TABLE_NAME, FieldName: "index_name", Type: constants.VARCHAR, Length: 16, Offset: 4},
				{TableName: INDEX_CATALOG_TABLE_NAME, FieldName: "table_name", Type: constants.VARCHAR, Length: 16, Offset: 24},
				{TableName: INDEX_CATALOG_TABLE_NAME, FieldName: "field_name", Type: constants.VARCHAR, Length: 16, Offset: 44},
			}

			assert.Subset(t, actualRows, expectedRows, "field_catalog テーブルに期待するレコードが入っていること.")
		})
	})
}

func TestIndexManagerGetCreateIndex(t *testing.T) {
	transaction := newTransactionForTest(t, indexManagerTestName)
	defer transaction.Rollback()

	tableManager := NewTableManager(true, transaction)
	statManager := NewStatManager(tableManager, transaction)

	indexManager := NewIndexManager(true, tableManager, statManager, transaction)

	// テスト用のテーブルを用意しておく.
	testTableName1 := types.TableName("test_idxmgr_1")
	testTableSchema1 := record.NewSchema()
	testTableSchema1.AddIntField("id")
	testTableSchema1.AddStringField("name", 10)
	tableManager.CreateTable(testTableName1, testTableSchema1, transaction)
	testTableName2 := types.TableName("test_idxmgr_2")
	testTableSchema2 := record.NewSchema()
	testTableSchema2.AddStringField("unit", 10)
	testTableSchema2.AddIntField("price")
	tableManager.CreateTable(testTableName2, testTableSchema2, transaction)

	testIndexName1 := types.IndexName("test_index_1")
	testIndexName2 := types.IndexName("test_index_2")

	t.Run("インデックスの作成が正常に行われる.", func(t *testing.T) {
		indexManager.CreateIndex(testIndexName1, testTableName1, "id", transaction)
		indexManager.CreateIndex(testIndexName2, testTableName1, "name", transaction)

		t.Run("インデックスカタログテーブルに期待するレコードが入っていること.", func(t *testing.T) {
			indexCatalogLayout, _ := tableManager.GetLayout(INDEX_CATALOG_TABLE_NAME, transaction)
			tableScan := record.NewTableScan(transaction, INDEX_CATALOG_TABLE_NAME, indexCatalogLayout)
			defer tableScan.Close()

			actualRows := make([]IndexCatalogRow, 0)
			for tableScan.Next() {
				actualRows = append(
					actualRows,
					IndexCatalogRow{
						IndexName: types.IndexName(tableScan.GetString("index_name")),
						TableName: types.TableName(tableScan.GetString("table_name")),
						FieldName: types.FieldName(tableScan.GetString("field_name")),
					},
				)
			}
			expectedRows := []IndexCatalogRow{
				{IndexName: testIndexName1, TableName: testTableName1, FieldName: "id"},
				{IndexName: testIndexName2, TableName: testTableName1, FieldName: "name"},
			}

			assert.Subset(t, actualRows, expectedRows, "インデックスカタログテーブルに期待するレコードが入っていること.")
		})
	})

	t.Run("作成したインデックスを取得できる.", func(t *testing.T) {
		indexInfoMap, err := indexManager.GetIndexInfo(testTableName1, transaction)
		if assert.NoError(t, err, "存在するテーブルなのでインデックス情報は取得できるべし.") {
			assert.Len(t, indexInfoMap, 2, "作成したインデックスの数だけ取得できる.")

			indexInfo1, ok1 := indexInfoMap["id"]
			indexInfo2, ok2 := indexInfoMap["name"]

			assert.True(t, ok1, "id というフィールド名のインデックス情報が取得できる.")
			assert.Equal(t, testIndexName1, indexInfo1.indexName, "インデックス名が期待した値である.")
			assert.Equal(t, types.FieldName("id"), indexInfo1.fieldName, "テーブル名が期待した値である.")

			assert.True(t, ok2, "name というフィールド名のインデックス情報が取得できる.")
			assert.Equal(t, testIndexName2, indexInfo2.indexName, "インデックス名が期待した値である.")
			assert.Equal(t, types.FieldName("name"), indexInfo2.fieldName, "テーブル名が期待した値である.")
		}
	})

	t.Run("インデックスが作成されていないテーブルからは、空のインデックス情報が取得できる.", func(t *testing.T) {
		indexInfoMap, err := indexManager.GetIndexInfo(testTableName2, transaction)
		if assert.NoError(t, err, "テーブル自体は存在するので、エラーにはならない.") {
			assert.Empty(t, indexInfoMap, "インデックス情報は空である.")
		}
	})

	t.Run("存在しないテーブルのインデックスを取得するとエラーになる.", func(t *testing.T) {
		indexInfoMap, err := indexManager.GetIndexInfo("hoge_table", transaction)
		if assert.Error(t, err, "存在しないテーブルのインデックス情報は取得できない.") {
			assert.IsType(t, CannotGetIndexInfoError{}, err, "存在しないテーブルのインデックス情報は CannotGetIndexInfoError を返すべし.")
			assert.Nil(t, indexInfoMap, "存在しないテーブルのインデックス情報は nil を返すべし.")
		}
	})
}
