package query

import (
	"simple-db-go/record"
	"simple-db-go/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProjectScanScanInterfaceMethods(t *testing.T) {
	transaction := newTransactionForTest(t, projectScanTestName)
	defer transaction.Rollback()
	metadataManager := startMetadataManagerForTest(t, projectScanTestName, transaction)

	// テスト用のテーブルを用意する. Projection だけなので1つで良い。
	testTableName := types.TableName("users")
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 10)
	schema.AddIntField("age")
	metadataManager.CreateTable(testTableName, schema, transaction)

	// このテスト用のテーブルにいくらかデータを入れる。100件くらい入れてみるか。
	layout := record.NewLayout(schema)
	tableScan := record.NewTableScan(transaction, testTableName, layout)
	for i := types.Int(0); i < 100; i++ {
		tableScan.Insert()
		tableScan.SetInt("id", i)
		tableScan.SetString("name", "name"+string(i%7))
		tableScan.SetInt("age", i%3)
	}
	tableScan.Close()

	t.Run("ProjectScan で指定したフィールドだけ取得できること.", func(t *testing.T) {
		tableScan := record.NewTableScan(transaction, testTableName, layout)
		projectScan := NewProjectScan(tableScan, []types.FieldName{"id", "name"})
		defer projectScan.Close()

		scannedRecordsCount := 0
		for i := types.Int(0); projectScan.Next(); i++ {
			idValue, idError := projectScan.GetInt("id")
			nameValue, nameError := projectScan.GetString("name")
			ageValue, ageError := projectScan.GetInt("age")

			if assert.NoError(t, idError) && assert.NoError(t, nameError) {
				assert.Equalf(t, i, idValue, "id が期待した値であること. i=%d", i)
				assert.Equalf(t, "name"+string(i%7), nameValue, "name が期待した値であること. i=%d", i)
			}

			if assert.Error(t, ageError, "`age`フィールドは ProjectScan で指定していないのでエラーを返すべし.") {
				assert.Equal(t, types.Int(0), ageValue, "`age`フィールドは ProjectScan で指定していないので、デフォルト値が返るべし.")
				assert.IsType(t, &UnknownFieldInProjectScanError{}, ageError, "`age`フィールドは ProjectScan で指定していないので、UnknownFieldInProjectScanError を返すべし.")
			}

			scannedRecordsCount++
		}

		assert.Equal(t, 100, scannedRecordsCount, "フィルタリングはしていないので、全てのレコードが取得できるべし.")
	})
}
