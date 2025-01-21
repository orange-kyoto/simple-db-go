package query_test

import (
	"simple-db-go/query"
	"simple-db-go/record"
	"simple-db-go/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProductScanScanInterfaceMethods(t *testing.T) {
	transaction := newTransactionForTest(t, productScanTestName)
	defer transaction.Rollback()
	metadataManager := startMetadataManagerForTest(t, productScanTestName, transaction)

	// テスト用のテーブルを2つ用意する。あとでJOINできる想定で準備する。
	testTableName1 := types.TableName("users")
	schema1 := record.NewSchema()
	schema1.AddIntField("id")
	schema1.AddStringField("name", 10)
	metadataManager.CreateTable(testTableName1, schema1, transaction)

	testTableName2 := types.TableName("orders")
	schema2 := record.NewSchema()
	schema2.AddIntField("order_id")
	schema2.AddIntField("user_id")
	schema2.AddIntField("product_id")
	metadataManager.CreateTable(testTableName2, schema2, transaction)

	// 2つのテーブルにデータを入れる。users には100件、orders には50件入れる。
	layout1 := record.NewLayout(schema1)
	tableScan1 := query.NewTableScan(transaction, testTableName1, layout1)
	for i := types.Int(0); i < 100; i++ {
		tableScan1.Insert()
		tableScan1.SetInt("id", i)
		tableScan1.SetString("name", "name"+string(i%7))
	}
	tableScan1.Close()

	layout2 := record.NewLayout(schema2)
	tableScan2 := query.NewTableScan(transaction, testTableName2, layout2)
	for i := types.Int(0); i < 50; i++ {
		tableScan2.Insert()
		tableScan2.SetInt("order_id", i)
		tableScan2.SetInt("user_id", i/4)
		tableScan2.SetInt("product_id", i%10)
	}
	tableScan2.Close()

	t.Run("2つのテーブルの積をすべて取得することができること.", func(t *testing.T) {
		tableScan1 := query.NewTableScan(transaction, testTableName1, layout1)
		tableScan2 := query.NewTableScan(transaction, testTableName2, layout2)
		productScan := query.NewProductScan(tableScan1, tableScan2)
		defer productScan.Close()

		scannedRecordsCount := 0
		for i := types.Int(0); productScan.Next(); i++ {
			idValue, idError := productScan.GetInt("id")
			nameValue, nameError := productScan.GetString("name")
			orderIdValue, orderIdError := productScan.GetInt("order_id")
			userIdValue, userIdError := productScan.GetInt("user_id")
			productIdValue, productIdError := productScan.GetInt("product_id")

			if assert.NoError(t, idError) && assert.NoError(t, nameError) && assert.NoError(t, orderIdError) && assert.NoError(t, userIdError) && assert.NoError(t, productIdError) {
				assert.Equal(t, types.Int(i/50), idValue, "id が期待した値であること.")
				assert.Equal(t, "name"+string((i/50)%7), nameValue, "name が期待した値であること.")
				assert.Equal(t, types.Int(i%50), orderIdValue, "order_id が期待した値であること.")
				assert.Equal(t, types.Int((i%50)/4), userIdValue, "user_id が期待した値であること.")
				assert.Equal(t, types.Int((i%50)%10), productIdValue, "product_id が期待した値であること.")
			}

			scannedRecordsCount++
		}

		assert.Equal(t, 100*50, scannedRecordsCount, "2つのテーブルの積をすべて取得できること.")
	})

	t.Run("SelectScanと組み合わせて、JOINに相当する操作ができること.", func(t *testing.T) {
		tableScan1 := query.NewTableScan(transaction, testTableName1, layout1)
		tableScan2 := query.NewTableScan(transaction, testTableName2, layout2)
		productScan := query.NewProductScan(tableScan1, tableScan2)
		defer productScan.Close()

		// `id = user_id` を想定し、ユーザーごとの注文情報を取得できることを期待する.
		term := query.NewTerm(query.NewFieldNameExpression("id"), query.NewFieldNameExpression("user_id"))
		predicate := query.NewPredicateWith(term)
		seletScan := query.NewSelectScan(productScan, predicate)

		scannedRecordsCount := 0
		for i := types.Int(0); seletScan.Next(); i++ {
			idValue, idError := seletScan.GetInt("id")
			nameValue, nameError := seletScan.GetString("name")
			orderIdValue, orderIdError := seletScan.GetInt("order_id")
			userIdValue, userIdError := seletScan.GetInt("user_id")
			productIdValue, productIdError := seletScan.GetInt("product_id")

			if assert.NoError(t, idError) && assert.NoError(t, nameError) && assert.NoError(t, orderIdError) && assert.NoError(t, userIdError) && assert.NoError(t, productIdError) {
				assert.True(t, idValue == userIdValue, "id と user_id が一致すること.")
				assert.Equal(t, types.Int(i/4), idValue, "id が期待した値であること.")
				assert.Equal(t, "name"+string((i/4)%7), nameValue, "name が期待した値であること.")
				assert.Equal(t, types.Int(i), orderIdValue, "order_id が期待した値であること.")
				assert.Equal(t, types.Int(i/4), userIdValue, "user_id が期待した値であること.")
				assert.Equal(t, types.Int(i%10), productIdValue, "product_id が期待した値であること.")
			}

			scannedRecordsCount++
		}

		assert.Equal(t, 50, scannedRecordsCount, "ユーザーごとの注文情報を取得できること(INNER JOINに相当するので、注文テーブルのレコード数と一致する.).")
	})
}
