package transaction

import (
	"os"
	"simple-db-go/buffer"
	"simple-db-go/config"
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/util"
	"testing"
)

const (
	bufferListTestName            = "buffer_list_test"
	transactionTestName           = "transaction_test"
	recoveryManagerTestName       = "recovery_manager_test"
	logFileNameForTransactionTest = transactionTestName + ".log"
	logFileNameForRMTest          = recoveryManagerTestName + ".log"
	dataFileNameForRMTest         = recoveryManagerTestName + ".data"
	blockSize                     = 512
)

func TestMain(m *testing.M) {
	util.Cleanup(recoveryManagerTestName)
	util.Cleanup(transactionTestName)
	util.Cleanup(bufferListTestName)

	code := m.Run()

	util.Cleanup(recoveryManagerTestName)
	util.Cleanup(transactionTestName)
	util.Cleanup(bufferListTestName)
	os.Exit(code)
}

func startBufferListForTest(t *testing.T) *BufferList {
	config := config.NewDBConfigForTest(t, bufferListTestName, blockSize, 10)
	fileManager := file.StartManagerForTest(bufferListTestName, config)
	logManager := log.StartManagerForTest(bufferListTestName, config, fileManager)
	bufferManager := buffer.StartManagerForTest(bufferListTestName, config, fileManager, logManager)

	return NewBufferList(bufferManager)
}

func startNewTransactionForTest(t *testing.T, testName string) *Transaction {
	config := config.NewDBConfigForTest(t, testName, blockSize, 10)
	return NewTransactionForTest(testName, config)
}

// Recover のテストのため、一度システムがクラッシュしたものと想定し、再起動することをシミュレートする.
// ここでは単に、manager 系をリセットすることにする.
func rebootDatabaseForTransactionTest(t *testing.T) {
	config := config.NewDBConfigForTest(t, transactionTestName, blockSize, 10)
	file.ResetManagerForTest(transactionTestName, config)
	log.ResetManagerForTest(transactionTestName, config)
	buffer.ResetManagerForTest(transactionTestName, config)

	// うまくないと思うが、一旦ロックテーブルをリセットしておく. 何か上手い仕組みを入れたいところ...
	lockTableInstance = &LockTable{
		locks:       make(map[file.BlockID]LockValue),
		requestChan: make(chan lockTableRequest),
		closeChan:   make(chan bool),
	}
	go lockTableInstance.run()
}
