package query

import (
	"os"
	"simple-db-go/config"
	"simple-db-go/file"
	"simple-db-go/metadata"
	"simple-db-go/transaction"
	"simple-db-go/util"
	"testing"
)

const (
	selectScanTestName  = "test_select_scan"
	projectScanTestName = "test_project_scan"
	productScanTestName = "test_product_scan"
)

func TestMain(m *testing.M) {
	util.Cleanup(selectScanTestName)
	util.Cleanup(projectScanTestName)
	util.Cleanup(productScanTestName)

	code := m.Run()

	util.Cleanup(selectScanTestName)
	util.Cleanup(projectScanTestName)
	util.Cleanup(productScanTestName)
	os.Exit(code)
}

func newTransactionForTest(t *testing.T, testName string) *transaction.Transaction {
	config := config.NewDBConfigForTest(t, testName, 512, 10)
	return transaction.NewTransactionForTest(testName, config)
}

func startMetadataManagerForTest(t *testing.T, testName string, transaction *transaction.Transaction) *metadata.MetadataManager {
	config := config.NewDBConfigForTest(t, testName, 512, 10)
	fileManager := file.GetManagerForTest(testName)

	return metadata.StartManagerForTest(testName, config, fileManager.IsNew(), transaction)
}
