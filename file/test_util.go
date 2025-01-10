//go:build test

package file

import (
	"fmt"
	"simple-db-go/config"
	"sync"
)

var (
	fileManagerStore = make(map[string]*FileManager)
	mu               sync.Mutex
)

// testName ごとに、FileManager を1つだけ生成する.
func StartManagerForTest(testName string, config config.DBConfig) *FileManager {
	mu.Lock()
	defer mu.Unlock()

	fileManager, exists := fileManagerStore[testName]
	if !exists {
		fileManager = NewFileManager(config.GetDBDirectory(), config.GetBlockSize())
		fileManagerStore[testName] = fileManager
	}

	return fileManager
}

func GetManagerForTest(testName string) *FileManager {
	mu.Lock()
	defer mu.Unlock()

	fileManager, exists := fileManagerStore[testName]
	if !exists {
		panic(fmt.Sprintf("ファイルマネージャーが存在しません. 先に transaction を作るなどして、マネージャーの初期化をするように修正してください. testName=%s", testName))
	}

	return fileManager
}

func ResetManagerForTest(testName string, config config.DBConfig) {
	mu.Lock()
	defer mu.Unlock()

	fileManager := NewFileManager(config.GetDBDirectory(), config.GetBlockSize())
	fileManagerStore[testName] = fileManager
}
