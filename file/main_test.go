package file

import (
	"os"
	"path"
	"simple-db-go/config"
	"simple-db-go/util"
	"testing"
)

const (
	fileManagerTestName = "file_manager_test"
	blockSize           = 32
)

func TestMain(m *testing.M) {
	util.Cleanup(fileManagerTestName)
	code := m.Run()
	util.Cleanup(fileManagerTestName)
	os.Exit(code)
}

func createTempFiles() {
	os.Mkdir(fileManagerTestName, 0755)
	os.Create(path.Join(fileManagerTestName, "tempfile1"))
	os.Create(path.Join(fileManagerTestName, "tempfile2"))
}

func getFileManagerForTest(t *testing.T) *FileManager {
	config := config.NewDBConfigForTest(t, fileManagerTestName, blockSize, 1)
	return StartManagerForTest(fileManagerTestName, config)
}
