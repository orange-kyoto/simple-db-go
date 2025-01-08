package record

import (
	"os"
	"simple-db-go/test_util"
	"testing"
)

const (
	recordPageTestName = "test_record_page"
	tableScanTestName  = "test_table_scan"
)

func TestMain(m *testing.M) {
	test_util.Cleanup(recordPageTestName)
	test_util.Cleanup(tableScanTestName)
	test_util.StartManagers(recordPageTestName, 512, 10)
	test_util.StartManagers(tableScanTestName, 512, 10)

	code := m.Run()

	test_util.Cleanup(recordPageTestName)
	test_util.Cleanup(tableScanTestName)
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
