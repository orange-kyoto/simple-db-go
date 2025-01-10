package metadata

import (
	"os"
	"simple-db-go/config"
	"simple-db-go/transaction"
	"simple-db-go/util"
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
		util.Cleanup(name)
	}

	code := m.Run()

	for _, name := range testNames {
		util.Cleanup(name)
	}
	os.Exit(code)
}

func newTransactionForTest(t *testing.T, testName string) *transaction.Transaction {
	config := config.NewDBConfigForTest(t, testName, 512, 10)
	return transaction.NewTransactionForTest(testName, config)
}
