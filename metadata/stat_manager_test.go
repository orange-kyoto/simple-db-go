package metadata

import (
	"fmt"
	"simple-db-go/record"
	"simple-db-go/test_util"
	"simple-db-go/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatManagerNewStatManager(t *testing.T) {
	t.Run("StatManager 初期化時に統計情報の初期値が計算される.", func(t *testing.T) {
		transaction := test_util.StartNewTransaction(statManagerTestName)
		defer transaction.Rollback()

		tableManager := NewTableManager(true, transaction)
		NewViewManager(true, tableManager, transaction)

		// 別のテーブルも1つ追加し、いくつかレコードを追加しておく.
		// これで、3つのカタログテーブル＋1つのテーブルが存在することになる.
		testTableName := types.TableName("test_statmanager")
		testTableSchema := record.NewSchema()
		testTableSchema.AddIntField("A")
		testTableSchema.AddStringField("B", 7)
		tableManager.CreateTable(testTableName, testTableSchema, transaction)
		testTableLayout, _ := tableManager.GetLayout(testTableName, transaction)
		testTableScan := record.NewTableScan(transaction, testTableName, testTableLayout)
		defer testTableScan.Close()
		for i := types.Int(0); i < 777; i++ {
			testTableScan.Insert()
			testTableScan.SetInt("A", i)
			testTableScan.SetString("B", fmt.Sprintf("test%d", i))
		}

		statManager := NewStatManager(tableManager, transaction)

		t.Run("StatManager のフィールドの大きさや値が適切に初期化されている.", func(t *testing.T) {
			assert.Len(t, statManager.tableStats, 4, "テーブルカタログテーブル＋フィールドカタログテーブル＋ビューカタログテーブル＋テストテーブルが存在するはず.")
			assert.Equal(t, types.Int(0), statManager.numCalls, "初期状態では numCalls は 0 であるはず.")
		})

		// GetStatInfo メソッドではなく、直接見てみる。

		t.Run("テーブルカタログテーブルの統計情報が正しく計算されている.", func(t *testing.T) {
			actualStatInfo, exists := statManager.tableStats[TABLE_CATALOG_TABLE_NAME]
			expectedStatInfo := &StatInfo{
				// blocksize 512, slot_size 28 なので、４レコードは1つのブロックに収まる.
				numBlocks: 1,
				// catalog テーブル3つ＋テストテーブル1つ
				numRecords: 4,
			}
			assert.True(t, exists, "テーブルカタログテーブルの統計情報が存在するはず.")
			assert.Equal(t, expectedStatInfo, actualStatInfo, "テーブルカタログテーブルの統計情報が正しいはず.")

			layout, _ := tableManager.GetLayout(TABLE_CATALOG_TABLE_NAME, transaction)
			statInfo := statManager.GetStatInfo(TABLE_CATALOG_TABLE_NAME, layout, transaction)
			assert.Equal(t, expectedStatInfo, statInfo, "GetStatInfo でも同じ統計情報が取得できるはず.")
			assert.Equal(t, types.Int(2), statInfo.GetDistinctValues("table_name"), "table_name は 2 つの値を取るはず.(雑な推測値を返すだけなので、厳密な値ではない)")
		})

		t.Run("フィールドカタログテーブルの統計情報が正しく計算されている.", func(t *testing.T) {
			actualStatInfo, exists := statManager.tableStats[FIELD_CATALOG_TABLE_NAME]
			expectedStatInfo := &StatInfo{
				// block size 512, slot_size 56 なので、11レコードを収めるには2つブロックが必要.
				numBlocks: 2,
				// 各テーブルのフィールド数：
				// - table_catalog: 2
				// - field_catalog: 5
				// - view_catalog: 2
				// - test_statmanager: 2
				// 以上の合計値になるはず.
				numRecords: 11,
			}
			assert.True(t, exists, "フィールドカタログテーブルの統計情報が存在するはず.")
			assert.Equal(t, expectedStatInfo, actualStatInfo, "フィールドカタログテーブルの統計情報が正しいはず.")

			layout, _ := tableManager.GetLayout(FIELD_CATALOG_TABLE_NAME, transaction)
			statInfo := statManager.GetStatInfo(FIELD_CATALOG_TABLE_NAME, layout, transaction)
			assert.Equal(t, expectedStatInfo, statInfo, "GetStatInfo でも同じ統計情報が取得できるはず.")
			assert.Equal(t, types.Int(4), statInfo.GetDistinctValues("table_name"), "table_name は 3 つの値を取るはず.(雑な推測値を返すだけなので、厳密な値ではない)")
		})

		t.Run("ビューカタログテーブルの統計情報が正しく計算されている.", func(t *testing.T) {
			actualStatInfo, exists := statManager.tableStats[VIEW_CATALOG_TABLE_NAME]
			expectedStatInfo := &StatInfo{
				// レコードが1つもないはずなので.
				numBlocks:  0,
				numRecords: 0,
			}
			assert.True(t, exists, "ビューカタログテーブルの統計情報が存在するはず.")
			assert.Equal(t, expectedStatInfo, actualStatInfo, "ビューカタログテーブルの統計情報が正しいはず.")

			layout, _ := tableManager.GetLayout(VIEW_CATALOG_TABLE_NAME, transaction)
			statInfo := statManager.GetStatInfo(VIEW_CATALOG_TABLE_NAME, layout, transaction)
			assert.Equal(t, expectedStatInfo, statInfo, "GetStatInfo でも同じ統計情報が取得できるはず.")
			assert.Equal(t, types.Int(1), statInfo.GetDistinctValues("table_name"), "table_name は 1 つの値を取るはず.(雑な推測値を返すだけなので、厳密な値ではない)")
		})

		t.Run("テスト用のテーブルの統計情報が正しく計算されている.", func(t *testing.T) {
			actualStatInfo, exists := statManager.tableStats[testTableName]
			expectedStatInfo := &StatInfo{
				// block_size 512, slot_size 19 (= flag: 4 + field A: 4 + field B: (4+7))
				// 1つのブロックに入るスロット数 = floor( 512 / 19 ) = 26
				// よって777レコード収めるのに必要なブロック数 = ceil( 777 / 26 ) = 30
				numBlocks: 30,
				// 777 個レコードをINSERTしたはずなので.
				numRecords: 777,
			}
			assert.True(t, exists, "テスト用のテーブルの統計情報が存在するはず.")
			assert.Equal(t, expectedStatInfo, actualStatInfo, "テスト用のテーブルの統計情報が正しいはず.")

			layout, _ := tableManager.GetLayout(testTableName, transaction)
			statInfo := statManager.GetStatInfo(testTableName, layout, transaction)
			assert.Equal(t, expectedStatInfo, statInfo, "GetStatInfo でも同じ統計情報が取得できるはず.")
			assert.Equal(t, types.Int(260), statInfo.GetDistinctValues("A"), "A は 2 つの値を取るはず.(雑な推測値を返すだけなので、厳密な値ではない)")
		})
	})
}

func TestStatManagerGetStatInfo(t *testing.T) {
	t.Run("GetStatInfo で統計情報を正しく取得できる.", func(t *testing.T) {
		t.Skip("NewStatManager のテストで実行しているのでよしとする.(こちらに移すべきかも？)")
	})

	t.Run("GetStatInfo が100回より多く参照されたときに統計情報が計算され直すこと.", func(t *testing.T) {
		transaction := test_util.StartNewTransaction(statManagerTestName)
		defer transaction.Rollback()

		tableManager := NewTableManager(true, transaction)
		statManager := NewStatManager(tableManager, transaction)

		// table_catalog テーブルの統計情報が更新されるかだけ確認する.細かい値までは確認しない.
		layout, _ := tableManager.GetLayout(TABLE_CATALOG_TABLE_NAME, transaction)
		initTableCatalogStatInfo := statManager.GetStatInfo(TABLE_CATALOG_TABLE_NAME, layout, transaction)

		t.Run("1~100回までは統計情報は更新されない.", func(t *testing.T) {
			// 注意：すぐ上で GetStatInfo を1度呼んでいることに注意.
			for i := 2; i <= 100; i++ {
				statInfo := statManager.GetStatInfo(TABLE_CATALOG_TABLE_NAME, layout, transaction)
				assert.Samef(t, initTableCatalogStatInfo, statInfo, "100回以下の呼び出しでは統計情報は更新されないはず.(count=%d)", i)
			}
		})

		t.Run("101回目に呼ばれた後は統計情報が更新されている.", func(t *testing.T) {
			statInfo := statManager.GetStatInfo(TABLE_CATALOG_TABLE_NAME, layout, transaction)
			assert.NotSame(t, initTableCatalogStatInfo, statInfo, "101回目の呼び出し後は統計情報が更新されている.")
			assert.Equal(t, initTableCatalogStatInfo, statInfo, "特にレコード操作はしていないので、統計値自体は変わらない.")
		})
	})
}
