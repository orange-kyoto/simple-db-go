package record

import (
	"os"
	"simple-db-go/config"
	"simple-db-go/transaction"
	"simple-db-go/util"
	"testing"
)

const (
	recordPageTestName = "test_record_page"
	tableScanTestName  = "test_table_scan"
)

func TestMain(m *testing.M) {
	util.Cleanup(recordPageTestName)
	util.Cleanup(tableScanTestName)

	code := m.Run()

	util.Cleanup(recordPageTestName)
	util.Cleanup(tableScanTestName)
	os.Exit(code)
}

// テスト用に users テーブルという仮のスキーマを作成する.
func buildTestTableSchema() *Schema {
	schema := NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 10)
	schema.AddIntField("age")
	return schema
}

func newTransactionForTest(t *testing.T, testName string) *transaction.Transaction {
	config := config.NewDBConfigForTest(t, testName, 512, 10)
	return transaction.NewTransactionForTest(testName, config)
}
