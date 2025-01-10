package buffer

import (
	"os"
	"simple-db-go/config"
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/types"
	"simple-db-go/util"
	"testing"
)

const (
	bufferManagerTestName = "buffer_manager_test"
	blockSize             = types.Int(16)
	bufferPoolSize        = types.Int(3)
)

func TestMain(m *testing.M) {
	util.Cleanup(bufferManagerTestName)
	code := m.Run()
	util.Cleanup(bufferManagerTestName)
	os.Exit(code)
}

func getBufferManagerForTest(t *testing.T) *BufferManager {
	config := config.NewDBConfigForTest(t, bufferManagerTestName, blockSize, bufferPoolSize)
	fileManager := file.StartManagerForTest(bufferManagerTestName, config)
	logManager := log.StartManagerForTest(bufferManagerTestName, config, fileManager)
	bufferManager := StartManagerForTest(bufferManagerTestName, config, fileManager, logManager)
	return bufferManager
}
