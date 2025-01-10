package log

import (
	"os"
	"simple-db-go/config"
	"simple-db-go/file"
	"simple-db-go/types"
	"simple-db-go/util"
	"testing"
)

const logManagerTestName = "test_log_manager"

const blockSize = types.Int(16)

func TestMain(m *testing.M) {
	util.Cleanup(logManagerTestName)
	code := m.Run()
	util.Cleanup(logManagerTestName)
	os.Exit(code)
}

func getLogManagerForTest(t *testing.T) *LogManager {
	config := config.NewDBConfigForTest(t, logManagerTestName, blockSize, 1)
	fileManager := file.StartManagerForTest(logManagerTestName, config)
	logManager := StartManagerForTest(logManagerTestName, config, fileManager)
	return logManager
}
