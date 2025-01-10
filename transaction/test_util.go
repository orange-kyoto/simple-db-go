//go:build test

package transaction

import (
	"simple-db-go/buffer"
	"simple-db-go/config"
	"simple-db-go/file"
	"simple-db-go/log"
	"sync"
)

var mu sync.Mutex

func NewTransactionForTest(testName string, config config.DBConfig) *Transaction {
	mu.Lock()
	defer mu.Unlock()

	fileManager := file.StartManagerForTest(testName, config)
	logManager := log.StartManagerForTest(testName, config, fileManager)
	bufferManager := buffer.StartManagerForTest(testName, config, fileManager, logManager)

	return NewTransaction(fileManager, logManager, bufferManager)
}
