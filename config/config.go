package config

import (
	"fmt"
	"os"
	"simple-db-go/types"
	"strconv"
)

type DBConfig interface {
	GetDBDirectory() string
	GetLogFileName() string
	GetBlockSize() types.Int
	GetBufferPoolSize() types.Int
}

// DBディレクトリやブロックサイズなど、DBのグローバルな設定情報を管理する.
// TODO: 環境変数から読むのではなく、設定ファイルから読むようにする. embed 使いたい.
type DBConfigImpl struct {
	// SimpleDB で扱うファイルを置くディレクトリ.
	dbDirectory string

	logFileName string

	blockSize types.Int

	bufferPoolSize types.Int
}

func NewDBConfig() DBConfig {
	dbConfigInstance := &DBConfigImpl{
		dbDirectory:    readDbDirectoty(),
		logFileName:    readLogFileName(),
		blockSize:      readBlockSize(),
		bufferPoolSize: readBufferPoolSize(),
	}

	return dbConfigInstance
}

func (dci *DBConfigImpl) GetDBDirectory() string {
	return dci.dbDirectory
}

func (dci *DBConfigImpl) GetLogFileName() string {
	return dci.logFileName
}

func (dci *DBConfigImpl) GetBlockSize() types.Int {
	return dci.blockSize
}

func (dci *DBConfigImpl) GetBufferPoolSize() types.Int {
	return dci.bufferPoolSize
}

const (
	SIMPLE_DB_DIRECTORY_ENV        = "SIMPLE_DB_DIRECTORY"
	SIMPLE_DB_LOG_FILE_NAME_ENV    = "SIMPLE_DB_LOG_FILE_NAME"
	SIMPLE_DB_BLOCK_SIZE_ENV       = "SIMPLE_DB_BLOCK_SIZE"
	SIMPLE_DB_BUFFER_POOL_SIZE_ENV = "SIMPLE_DB_BUFFER_POOL_SIZE"
)

func readDbDirectoty() string {
	return readStringConfig("SIMPLE_DB_DIRECTORY")
}

func readLogFileName() string {
	return readStringConfig("SIMPLE_DB_LOG_FILE_NAME")
}

func readBlockSize() types.Int {
	return readIntConfig("SIMPLE_DB_BLOCK_SIZE")
}

func readBufferPoolSize() types.Int {
	return readIntConfig("SIMPLE_DB_BUFFER_POOL_SIZE")
}

func readStringConfig(envVar string) string {
	value := os.Getenv(envVar)
	if value == "" {
		panic(fmt.Sprintf("SimpleDB の設定の読み取りに失敗しました: %s is not set", envVar))
	}
	return value
}

func readIntConfig(envVar string) types.Int {
	value := os.Getenv(envVar)
	if value == "" {
		panic(fmt.Sprintf("SimpleDB の設定の読み取りに失敗しました: %s is not set", envVar))
	}

	intValue, err := strconv.Atoi(value)
	if err != nil {
		panic(fmt.Sprintf("SimpleDB の設定の読み取りに失敗しました: %s is not a valid integer. got=%s, err=%+v", envVar, value, err))
	}

	return types.Int(intValue)
}
