//go:build test

package config

import (
	"simple-db-go/types"
	"testing"
)

func NewDBConfigForTest(t *testing.T, testName string, blockSize types.Int, bufferPoolSize types.Int) DBConfig {
	t.Setenv(SIMPLE_DB_DIRECTORY_ENV, testName)
	t.Setenv(SIMPLE_DB_LOG_FILE_NAME_ENV, testName+".log")
	t.Setenv(SIMPLE_DB_BLOCK_SIZE_ENV, blockSize.ToString())
	t.Setenv(SIMPLE_DB_BUFFER_POOL_SIZE_ENV, bufferPoolSize.ToString())
	return NewDBConfig()
}
