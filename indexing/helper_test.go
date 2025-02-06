package indexing_test

import (
	"simple-db-go/config"
	"simple-db-go/file"
	"simple-db-go/metadata"
	"simple-db-go/record"
	"simple-db-go/transaction"
	"testing"
)

const (
	blockSize      = 4096
	bufferPoolSize = 100
)

func newTransactionForTest(t *testing.T, testName string) *transaction.Transaction {
	config := config.NewDBConfigForTest(t, testName, blockSize, bufferPoolSize)
	return transaction.NewTransactionForTest(testName, config)
}

func getMetadamanagerForTest(t *testing.T, testName string, transaction *transaction.Transaction) *metadata.MetadataManager {
	fileManager := file.GetManagerForTest(testName)
	config := config.NewDBConfigForTest(t, testName, blockSize, bufferPoolSize)
	return metadata.StartManagerForTest(testName, config, fileManager.IsNew(), transaction)
}

func createUserTable(metadataManager *metadata.MetadataManager, transaction *transaction.Transaction) {
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 10)
	schema.AddIntField("age")
	metadataManager.CreateTable("users", schema, transaction)
}

func createUserTableIndex(metadataManager *metadata.MetadataManager, transaction *transaction.Transaction) {
	metadataManager.CreateIndex("ididx", "users", "id", transaction)
	metadataManager.CreateIndex("nameidx", "users", "name", transaction)
	metadataManager.CreateIndex("ageidx", "users", "age", transaction)
}
