package metadata

import (
	"os"
	"simple-db-go/test_util"
	"testing"
)

const tableManagerTestName = "table_manager_test"
const viewManagerTestName = "view_manager_test"

func TestMain(m *testing.M) {
	test_util.Cleanup(tableManagerTestName)
	test_util.Cleanup(viewManagerTestName)

	test_util.StartManagers(tableManagerTestName, 512, 10)
	test_util.StartManagers(viewManagerTestName, 512, 10)

	code := m.Run()

	test_util.Cleanup(tableManagerTestName)
	test_util.Cleanup(viewManagerTestName)
	os.Exit(code)
}
