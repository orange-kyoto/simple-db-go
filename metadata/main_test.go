package metadata

import (
	"os"
	"simple-db-go/test_util"
	"testing"
)

const tableManagerTestName = "table_manager_test"

func TestMain(m *testing.M) {
	test_util.Cleanup(tableManagerTestName)
	test_util.StartManagers(tableManagerTestName, 512, 10)

	code := m.Run()

	test_util.Cleanup(tableManagerTestName)
	os.Exit(code)
}
