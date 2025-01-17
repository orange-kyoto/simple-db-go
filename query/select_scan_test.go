package query

import (
	"simple-db-go/record"
	"simple-db-go/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelectScanScanInterfaceMethods(t *testing.T) {
	transaction := newTransactionForTest(t, selectScanTestName)
	defer transaction.Rollback()
	metadataManager := startMetadataManagerForTest(t, selectScanTestName, transaction)

	// テスト用のテーブルを用意する. Select だけなので1つで良い。
	testTableName := types.TableName("users")
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 10)
	schema.AddIntField("age")
	schema.AddStringField("country", 5)
	metadataManager.CreateTable(testTableName, schema, transaction)

	// このテスト用のテーブルにいくらかデータを入れる。100件くらい入れてみるか。
	layout := record.NewLayout(schema)
	tableScan := record.NewTableScan(transaction, testTableName, layout)
	for i := types.Int(0); i < 100; i++ {
		tableScan.Insert()
		tableScan.SetInt("id", i)
		tableScan.SetString("name", "name"+string(i%7))
		tableScan.SetInt("age", i%3)
		tableScan.SetString("country", "Japan")
	}
	tableScan.Close()

	t.Run("常に TRUE となる Predicate の場合、すべてのレコードを取得できる.", func(t *testing.T) {
		// `country = 'Japan'` を想定.
		testTerm := NewTerm(NewFieldNameExpression("country"), NewConstExpression(record.NewStrConstant("Japan")))
		testPredicate := NewPredicateWith(testTerm)

		tableScan := record.NewTableScan(transaction, testTableName, layout)
		selectScan := NewSelectScan(tableScan, testPredicate)
		defer selectScan.Close()

		scannedRecordsCount := 0
		for i := types.Int(0); selectScan.Next(); i++ {
			idValue, idError := selectScan.GetInt("id")
			nameValue, nameError := selectScan.GetString("name")
			ageValue, ageError := selectScan.GetInt("age")
			countryValue, countryError := selectScan.GetString("country")

			if assert.NoError(t, idError) && assert.NoError(t, nameError) && assert.NoError(t, ageError) && assert.NoError(t, countryError) {
				assert.Equalf(t, i, idValue, "id が期待した値であること. i=%d", i)
				assert.Equalf(t, "name"+string(i%7), nameValue, "name が期待した値であること. i=%d", i)
				assert.Equalf(t, i%3, ageValue, "age が期待した値であること. i=%d", i)
				assert.Equalf(t, "Japan", countryValue, "country が期待した値であること. i=%d", i)
			}

			scannedRecordsCount++
		}

		assert.Equal(t, 100, scannedRecordsCount, "すべてのレコードを取得できること.")
	})

	t.Run("条件にマッチするレコードだけを取得することができること.", func(t *testing.T) {
		// `age = 2` を想定. 0~99のうち、３で割った時のあまりが2のレコードだけ取得したい.
		testTerm := NewTerm(NewFieldNameExpression("age"), NewConstExpression(record.NewIntConstant(2)))
		testPredicate := NewPredicateWith(testTerm)

		tableScan := record.NewTableScan(transaction, testTableName, layout)
		selectScan := NewSelectScan(tableScan, testPredicate)
		defer selectScan.Close()

		scannedRecordsCount := 0
		for i := types.Int(0); selectScan.Next(); i++ {
			idValue, idError := selectScan.GetInt("id")
			nameValue, nameError := selectScan.GetString("name")
			ageValue, ageError := selectScan.GetInt("age")
			countryValue, countryError := selectScan.GetString("country")

			if assert.NoError(t, idError) && assert.NoError(t, nameError) && assert.NoError(t, ageError) && assert.NoError(t, countryError) {
				assert.Equalf(t, i*3+2, idValue, "id が期待した値であること. i=%d", i)
				assert.Equalf(t, "name"+string((i*3+2)%7), nameValue, "name が期待した値であること. i=%d", i)
				assert.Equalf(t, types.Int(2), ageValue, "age が期待した値であること. i=%d", i)
				assert.Equalf(t, "Japan", countryValue, "country が期待した値であること. i=%d", i)
			}

			scannedRecordsCount++
		}

		assert.Equal(t, 33, scannedRecordsCount, "条件にマッチするレコードだけを取得できること.")
	})

	t.Run("条件にマッチするレコードが１件もない場合、Next() が直ちに false を返すこと.", func(t *testing.T) {
		// `age = 999`を想定。これを満たすレコードはない。
		testTerm := NewTerm(NewFieldNameExpression("age"), NewConstExpression(record.NewIntConstant(999)))
		testPredicate := NewPredicateWith(testTerm)

		tableScan := record.NewTableScan(transaction, testTableName, layout)
		selectScan := NewSelectScan(tableScan, testPredicate)
		defer selectScan.Close()

		assert.False(t, selectScan.Next(), "条件にマッチするレコードが１件もない場合、Next() が直ちに false を返すべし.")
	})

	// TODO: これはきちんとハンドリングしたい。ただ、Next() の変更はちょっと面倒なので後回し。
	t.Run("Predicate にて存在しない列名を含む条件を定義するとpanicする（仕様について要検討）", func(t *testing.T) {
		// `hoge = 999` を想定。hoge という列名は存在しない。
		testTerm := NewTerm(NewFieldNameExpression("hoge"), NewConstExpression(record.NewIntConstant(999)))
		testPredicate := NewPredicateWith(testTerm)

		tableScan := record.NewTableScan(transaction, testTableName, layout)
		selectScan := NewSelectScan(tableScan, testPredicate)
		defer selectScan.Close()

		assert.Panics(t, func() { selectScan.Next() }, "存在しない列名を含む条件を定義するとpanicするべし.エラーハンドリングの改善を検討する.")
	})
}
