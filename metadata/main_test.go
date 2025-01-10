package metadata

import (
	"os"
	"simple-db-go/test_util"
	"testing"
)

const tableManagerTestName = "table_manager_test"
const viewManagerTestName = "view_manager_test"
const statManagerTestName = "stat_manager_test"
const indexInfoTestName = "index_info_test"
const indexManagerTestName = "index_manager_test"

func TestMain(m *testing.M) {
	testNames := []string{
		tableManagerTestName,
		viewManagerTestName,
		statManagerTestName,
		indexInfoTestName,
		indexManagerTestName,
	}

	for _, name := range testNames {
		test_util.Cleanup(name)
		test_util.StartManagers(name, 512, 10)
	}

	code := m.Run()

	for _, name := range testNames {
		test_util.Cleanup(name)
	}
	os.Exit(code)
}
