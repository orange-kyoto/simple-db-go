//go:build test

package test_util

import (
	"fmt"
	"os"
	"simple-db-go/buffer"
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/transaction"
	"simple-db-go/types"
	"sync"
)

var (
	fileManagerStore   = make(map[string]*file.FileManager)
	logManagerStore    = make(map[string]*log.LogManager)
	bufferManagerStore = make(map[string]*buffer.BufferManager)
	mu                 sync.Mutex
)

// testName ごとに、fileManager, logManager, bufferManager を1つだけ生成する.
func StartManagers(testName string, blockSize types.Int, bufferPoolSize types.Int) (*file.FileManager, *log.LogManager, *buffer.BufferManager) {
	mu.Lock()
	defer mu.Unlock()

	fileManager, exists := fileManagerStore[testName]
	if !exists {
		fileManager = file.NewFileManager(testName, blockSize)
		fileManagerStore[testName] = fileManager
	}

	logManager, exists := logManagerStore[testName]
	if !exists {
		logManager = log.NewLogManager(fileManager, testName+".log")
		logManagerStore[testName] = logManager
	}

	bufferManager, exists := bufferManagerStore[testName]
	if !exists {
		bufferManager = buffer.NewBufferManager(fileManager, logManager, bufferPoolSize)
		bufferManagerStore[testName] = bufferManager
	}

	return fileManager, logManager, bufferManager
}

func GetManagers(testName string) (*file.FileManager, *log.LogManager, *buffer.BufferManager) {
	mu.Lock()
	defer mu.Unlock()

	fileManager, exists := fileManagerStore[testName]
	if !exists {
		panic(fmt.Sprintf("fileManager for %s does not exist", testName))
	}

	logManager, exists := logManagerStore[testName]
	if !exists {
		panic(fmt.Sprintf("logManager for %s does not exist", testName))
	}

	bufferManager, exists := bufferManagerStore[testName]
	if !exists {
		panic(fmt.Sprintf("bufferManager for %s does not exist", testName))
	}

	return fileManager, logManager, bufferManager
}

// NOTE: StartManagers を事前に呼ぶことを必須とする.
func StartNewTransaction(testName string) *transaction.Transaction {
	mu.Lock()
	defer mu.Unlock()

	fileManager, exists := fileManagerStore[testName]
	if !exists {
		panic(fmt.Sprintf("fileManager for %s does not exist", testName))
	}

	logManager, exists := logManagerStore[testName]
	if !exists {
		panic(fmt.Sprintf("logManager for %s does not exist", testName))
	}

	bufferManager, exists := bufferManagerStore[testName]
	if !exists {
		panic(fmt.Sprintf("bufferManager for %s does not exist", testName))
	}

	return transaction.NewTransaction(fileManager, logManager, bufferManager)
}

func Cleanup(path string) {
	os.RemoveAll(path)
}

// 指定した個数分の BlockID を用意する.
// BlockNumber は 0 から始まる連番.
func PrepareBlockIDs(num types.Int, fileName string) []*file.BlockID {
	result := make([]*file.BlockID, num)

	for i := types.Int(0); i < num; i++ {
		result[i] = file.NewBlockID(fileName, types.BlockNumber(i))
	}

	return result
}

// 指定した個数分の Page を用意する.
func PreparePages(num types.Int, blockSize types.Int) []*file.Page {
	result := make([]*file.Page, num)

	for i := types.Int(0); i < num; i++ {
		result[i] = file.NewPage(blockSize)
	}

	return result
}
