//go:build test

package metadata

import (
	"simple-db-go/config"
	"simple-db-go/transaction"
	"sync"
)

var (
	metadataManagerStore = make(map[string]*MetadataManager)
	mu                   sync.Mutex
)

func StartManagerForTest(testName string, config config.DBConfig, isNew bool, transaction *transaction.Transaction) *MetadataManager {
	mu.Lock()
	defer mu.Unlock()

	metadataManager, exists := metadataManagerStore[testName]
	if exists {
		return metadataManager
	}

	metadataManager = NewMetadataManager(isNew, transaction)
	metadataManagerStore[testName] = metadataManager
	return metadataManager
}
