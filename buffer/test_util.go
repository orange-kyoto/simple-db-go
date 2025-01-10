//go:build test

package buffer

import (
	"simple-db-go/config"
	"simple-db-go/file"
	"simple-db-go/log"
	"sync"
)

var (
	bufferManagerStore = make(map[string]*BufferManager)
	mu                 sync.Mutex
)

func StartManagerForTest(testName string, config config.DBConfig, fileManager *file.FileManager, logManager *log.LogManager) *BufferManager {
	mu.Lock()
	defer mu.Unlock()

	bufferManager, exists := bufferManagerStore[testName]
	if !exists {
		bufferManager = NewBufferManager(fileManager, logManager, config.GetBufferPoolSize())
		bufferManagerStore[testName] = bufferManager
	}

	return bufferManager
}

func GetManagerForTest(testName string) *BufferManager {
	mu.Lock()
	defer mu.Unlock()

	bufferManager, exists := bufferManagerStore[testName]
	if !exists {
		panic("バッファーマネージャーが存在しません. 先に transaction を作るなどして、マネージャーの初期化をするように修正してください.")
	}

	return bufferManager
}

func ResetManagerForTest(testName string, config config.DBConfig) {
	mu.Lock()
	defer mu.Unlock()

	fileManager := file.GetManagerForTest(testName)
	logManager := log.GetManagerForTest(testName)
	bufferManager := NewBufferManager(fileManager, logManager, config.GetBufferPoolSize())
	bufferManagerStore[testName] = bufferManager
}
