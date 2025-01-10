//go:build test

package log

import (
	"simple-db-go/config"
	"simple-db-go/file"
	"sync"
)

var (
	logManagerStore = make(map[string]*LogManager)
	mu              sync.Mutex
)

func StartManagerForTest(testName string, config config.DBConfig, fileManager *file.FileManager) *LogManager {
	mu.Lock()
	defer mu.Unlock()

	logManager, exists := logManagerStore[testName]
	if !exists {
		logManager = NewLogManager(fileManager, config.GetLogFileName())
		logManagerStore[testName] = logManager
	}

	return logManager
}

func GetManagerForTest(testName string) *LogManager {
	mu.Lock()
	defer mu.Unlock()

	logManager, exists := logManagerStore[testName]
	if !exists {
		panic("ログマネージャーが存在しません. 先に transaction を作るなどして、マネージャーの初期化をするように修正してください.")
	}

	return logManager
}

func ResetManagerForTest(testName string, config config.DBConfig) {
	mu.Lock()
	defer mu.Unlock()

	fileManager := file.GetManagerForTest(testName)
	logManager := NewLogManager(fileManager, config.GetLogFileName())
	logManagerStore[testName] = logManager
}
