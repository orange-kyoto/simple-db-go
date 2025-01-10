package db

import (
	"simple-db-go/buffer"
	"simple-db-go/config"
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/metadata"
	"simple-db-go/transaction"
	"sync"
)

type SimpleDB struct {
	fileManager     *file.FileManager
	logManager      *log.LogManager
	bufferManager   *buffer.BufferManager
	metadataManager *metadata.MetadataManager
}

var (
	simpleDBInstance *SimpleDB
	simpleDBOnce     sync.Once
)

func NewSimpleDB(config config.DBConfig) *SimpleDB {
	simpleDBOnce.Do(func() {
		fileManager := file.NewFileManager(config.GetDBDirectory(), config.GetBlockSize())
		logManager := log.NewLogManager(fileManager, config.GetLogFileName())
		bufferManager := buffer.NewBufferManager(fileManager, logManager, config.GetBufferPoolSize())

		transaction := transaction.NewTransaction(fileManager, logManager, bufferManager)
		defer transaction.Commit()

		isNew := fileManager.IsNew()
		if !isNew {
			transaction.Recover()
		}

		metadataManager := metadata.NewMetadataManager(isNew, transaction)

		simpleDBInstance = &SimpleDB{
			fileManager:     fileManager,
			logManager:      logManager,
			bufferManager:   bufferManager,
			metadataManager: metadataManager,
		}
	})

	return simpleDBInstance
}

func (sb *SimpleDB) GetFileManager() *file.FileManager {
	return sb.fileManager
}

func (sb *SimpleDB) GetLogManager() *log.LogManager {
	return sb.logManager
}

func (sb *SimpleDB) GetBufferManager() *buffer.BufferManager {
	return sb.bufferManager
}

func (sb *SimpleDB) GetMetadataManager() *metadata.MetadataManager {
	return sb.metadataManager
}

func (sb *SimpleDB) NewTransaction() *transaction.Transaction {
	return transaction.NewTransaction(sb.fileManager, sb.logManager, sb.bufferManager)
}
