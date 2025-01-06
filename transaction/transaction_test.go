package transaction

import (
	"os"
	"path"
	"simple-db-go/buffer"
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/types"
	"simple-db-go/util"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	testDirForTransactionTest        = "test_transaction"
	logFileNameForTransactionTest    = "test_transaction.log"
	dataFileNameForTransactionTest   = "test_transaction.data"
	blockSizeForTransactionTest      = 512
	bufferPoolSizeForTransactionTest = 10
)

var (
	fileManagerForTransactionTest   *file.FileManager
	logManagerForTransactionTest    *log.LogManager
	bufferManagerForTransactionTest *buffer.BufferManager
	muForTransactionTest            sync.Mutex
)

func startManagersForTransactionTest() {
	muForTransactionTest.Lock()
	defer muForTransactionTest.Unlock()

	if fileManagerForTransactionTest == nil {
		fileManagerForTransactionTest = file.NewFileManager(testDirForTransactionTest, blockSizeForTransactionTest)
	}
	if logManagerForTransactionTest == nil {
		logManagerForTransactionTest = log.NewLogManager(fileManagerForTransactionTest, logFileNameForTransactionTest)
	}
	if bufferManagerForTransactionTest == nil {
		bufferManagerForTransactionTest = buffer.NewBufferManager(fileManagerForTransactionTest, logManagerForTransactionTest, bufferPoolSizeForTransactionTest)
	}
}

func startNewTransaction() (*file.FileManager, *log.LogManager, *buffer.BufferManager, *Transaction) {
	startManagersForTransactionTest()

	t := NewTransaction(fileManagerForTransactionTest, logManagerForTransactionTest, bufferManagerForTransactionTest)
	return fileManagerForTransactionTest, logManagerForTransactionTest, bufferManagerForTransactionTest, t
}

// Recover のテストのため、一度システムがクラッシュしたものと想定し、再起動することをシミュレートする.
// ここでは単に、manager 系をリセットすることにする.
func rebootDatabaseForTransactionTest() {
	muForTransactionTest.Lock()
	defer muForTransactionTest.Unlock()

	fileManagerForTransactionTest = file.NewFileManager(testDirForTransactionTest, blockSizeForTransactionTest)
	logManagerForTransactionTest = log.NewLogManager(fileManagerForTransactionTest, logFileNameForTransactionTest)
	bufferManagerForTransactionTest = buffer.NewBufferManager(fileManagerForTransactionTest, logManagerForTransactionTest, bufferPoolSizeForTransactionTest)

	// うまくないと思うが、一旦ロックテーブルをリセットしておく. 何か上手い仕組みを入れたいところ...
	lockTableInstance = &LockTable{
		locks:       make(map[file.BlockID]LockValue),
		requestChan: make(chan lockTableRequest),
		closeChan:   make(chan bool),
	}
	go lockTableInstance.run()
}

func cleanup() {
	os.RemoveAll(testDirForRMTest)
	os.RemoveAll(testDirForTransactionTest)
	os.RemoveAll(testDirForBufferList)
}

func TestMain(m *testing.M) {
	cleanup()
	code := m.Run()
	cleanup()
	os.Exit(code)
}

func TestTransactionCommit(t *testing.T) {
	t.Skip("RecoveryManager 等のテストをしているのでスキップする.")
}

func TestTransactionGetSetString(t *testing.T) {
	fileManager, _, _, transaction := startNewTransaction()

	// テスト用の書き換え対象ファイルを準備しておく.
	fileName := "test_transaction_get_set_string.data"
	testBlockID1 := file.NewBlockID(fileName, 0)
	testBlockID2 := file.NewBlockID(fileName, 1)
	testBlockID3 := file.NewBlockID(fileName, 2)
	testPage1 := file.NewPage(blockSizeForTransactionTest)
	testPage2 := file.NewPage(blockSizeForTransactionTest)
	testPage3 := file.NewPage(blockSizeForTransactionTest)
	testPage1.SetString(0, "test1")
	testPage2.SetString(0, "test2")
	testPage3.SetString(0, "test3")
	fileManager.Write(testBlockID1, testPage1)
	fileManager.Write(testBlockID2, testPage2)
	fileManager.Write(testBlockID3, testPage3)

	// トランザクション内で書き換えを行う.
	transaction.Pin(testBlockID1)
	transaction.Pin(testBlockID2)
	transaction.Pin(testBlockID3)
	defer transaction.Unpin(testBlockID1)
	defer transaction.Unpin(testBlockID2)
	defer transaction.Unpin(testBlockID3)
	transaction.SetString(testBlockID1, 0, "test1-1", true)
	transaction.SetString(testBlockID3, 0, "test3-1", true)

	assert.Equal(t, "test1-1", transaction.GetString(testBlockID1, 0), "トランザクション内で書き換えた値が取得できる.")
	assert.Equal(t, "test2", transaction.GetString(testBlockID2, 0), "トランザクション内で書き換えていない値は元の値が取得できる.")
	assert.Equal(t, "test3-1", transaction.GetString(testBlockID3, 0), "トランザクション内で書き換えた値が取得できる.")
}

func TestTransactionRollback(t *testing.T) {
	fileManager, _, _, transaction1 := startNewTransaction()

	// テスト用の書き換え対象ファイルを準備しておく.
	fileName := "test_transaction_rollback.data"
	testBlockID1 := file.NewBlockID(fileName, 0)
	testBlockID2 := file.NewBlockID(fileName, 1)
	testBlockID3 := file.NewBlockID(fileName, 2)
	testPage1 := file.NewPage(blockSizeForTransactionTest)
	testPage2 := file.NewPage(blockSizeForTransactionTest)
	testPage3 := file.NewPage(blockSizeForTransactionTest)
	testPage1.SetString(0, "test1")
	testPage2.SetString(0, "test2")
	testPage3.SetString(0, "test3")
	fileManager.Write(testBlockID1, testPage1)
	fileManager.Write(testBlockID2, testPage2)
	fileManager.Write(testBlockID3, testPage3)

	// トランザクション内で書き換えを行う.
	transaction1.Pin(testBlockID1)
	transaction1.Pin(testBlockID2)
	transaction1.Pin(testBlockID3)
	transaction1.SetString(testBlockID1, 0, "test1-1", true)
	transaction1.SetString(testBlockID3, 0, "test3-1", true)

	// 別のトランザクションでも書き換えを行う.
	_, _, _, transaction2 := startNewTransaction()
	transaction2.Pin(testBlockID2)
	transaction2.SetString(testBlockID2, 0, "test2-1", true)

	// 先にバッファーのアドレスを保持しておく(Commit, Rollbackで解放されてしまうので)
	buffer1 := transaction1.bufferList.GetBuffer(testBlockID1)
	buffer2 := transaction2.bufferList.GetBuffer(testBlockID2)
	buffer3 := transaction1.bufferList.GetBuffer(testBlockID3)

	// トランザクション2 は Commit する.
	transaction2.Commit()

	// トランザクション1 は Rollback する.
	transaction1.Rollback()

	t.Run("transaction1 が実行した変更が巻き戻されている.", func(t *testing.T) {
		// ディスク内のファイルを読み込んで変更が反映されていないことを確認.
		page1 := file.NewPage(blockSizeForTransactionTest)
		page3 := file.NewPage(blockSizeForTransactionTest)
		fileManager.Read(testBlockID1, page1)
		fileManager.Read(testBlockID3, page3)

		assert.Equal(t, "test1", page1.GetString(0), "testBlockID1 の変更がディスクに反映されていない.")
		assert.Equal(t, "test3", page3.GetString(0), "testBlockID3 の変更がディスクに反映されていない.")

		// BufferManager 内のバッファーもチェック. それが巻き戻されていることが大事.
		assert.Equal(t, "test1", buffer1.GetContents().GetString(0), "testBlockID1 の変更がバッファーに反映されていない.")
		assert.Equal(t, "test3", buffer3.GetContents().GetString(0), "testBlockID3 の変更がバッファーに反映されていない.")

		// BufferList は全て Unpin されていること.
		assert.Empty(t, transaction1.bufferList.buffers, "Rollback すると buffers は空になる.")
		assert.Empty(t, transaction1.bufferList.pins, "Rollback すると pins は空になる.")
		assert.False(t, buffer1.IsPinned(), "Rollback するとバッファーはUnpinされている.")
		assert.False(t, buffer3.IsPinned(), "Rollback するとバッファーはUnpinされている.")

		// ロックが解放されていること.
		assert.Empty(t, transaction1.concurrencyManager.locks, "Rollback すると ConcurrencyManager.locks は空になる.")
	})

	t.Run("トランザクション2の変更はディスクに書き込まれ、巻き戻されていない.", func(t *testing.T) {
		page2 := file.NewPage(blockSizeForTransactionTest)
		fileManager.Read(testBlockID2, page2)

		// 変更がディスクに書き込まれていること.
		assert.Equal(t, "test2-1", page2.GetString(0), "testBlockID2 の変更がディスクに反映されている.")

		// 変更がバッファーに反映されていること.
		assert.Equal(t, "test2-1", buffer2.GetContents().GetString(0), "testBlockID2 の変更がバッファーに反映されている.")

		// Buffer は Unpin されていること.
		assert.False(t, buffer2.IsPinned(), "Commit するとバッファーはUnpinされている.")

		// ロックが解放されていること.
		assert.Empty(t, transaction2.concurrencyManager.locks, "Commit すると ConcurrencyManager.locks は空になる.")
	})

	t.Run("Commit, Rollback のログが記録されていること.", func(t *testing.T) {
		logFileBlockSize := fileManager.GetBlockLength(logFileNameForTransactionTest)
		latestLogBlockID := file.NewBlockID(logFileNameForTransactionTest, logFileBlockSize-1)
		latestLogPage := file.NewPage(blockSizeForTransactionTest)
		fileManager.Read(latestLogBlockID, latestLogPage)

		boundary := latestLogPage.GetInt(0)
		lastLog := log.RawLogRecord(latestLogPage.GetBytes(boundary))

		actualCommitLogRecordOffset := boundary + (file.Int32ByteSize + util.Len(lastLog))
		secondLastLog := log.RawLogRecord(latestLogPage.GetBytes(actualCommitLogRecordOffset))

		rollbackLogRecordSize := types.Int(8)
		expectedRollbackLogRecord := file.NewPage(rollbackLogRecordSize)
		expectedRollbackLogRecord.SetInt(0, types.Int(ROLLBACK))
		expectedRollbackLogRecord.SetInt(file.Int32ByteSize, types.Int(transaction1.transactionNumber))

		commitLogRecordSize := types.Int(8)
		expectedCommitLogRecord := file.NewPage(commitLogRecordSize)
		expectedCommitLogRecord.SetInt(0, types.Int(COMMIT))
		expectedCommitLogRecord.SetInt(file.Int32ByteSize, types.Int(transaction2.transactionNumber))

		assert.Equal(t, log.RawLogRecord(expectedRollbackLogRecord.Data), lastLog, "Rollback のログが最後に記録されている.")
		assert.Equal(t, log.RawLogRecord(expectedCommitLogRecord.Data), secondLastLog, "Commit のログが最後から2番目に記録されている.")
	})
}

func TestTransactionRecover(t *testing.T) {
	fileManager, logManager, bufferManager, transaction1 := startNewTransaction()
	_, _, _, transaction2 := startNewTransaction()
	_, _, _, transaction3 := startNewTransaction()

	// テスト用の書き換え対象ファイルを準備しておく.
	fileName := "test_transaction_recover.data"
	testBlockID1 := file.NewBlockID(fileName, 0)
	testBlockID2 := file.NewBlockID(fileName, 1)
	testBlockID3 := file.NewBlockID(fileName, 2)
	testPage1 := file.NewPage(blockSizeForTransactionTest)
	testPage2 := file.NewPage(blockSizeForTransactionTest)
	testPage3 := file.NewPage(blockSizeForTransactionTest)
	testPage1.SetString(0, "hoge1")
	testPage2.SetString(0, "fuga2")
	testPage3.SetString(0, "piyo3")
	fileManager.Write(testBlockID1, testPage1)
	fileManager.Write(testBlockID2, testPage2)
	fileManager.Write(testBlockID3, testPage3)

	// トランザクション内で書き換えを行う.
	transaction1.Pin(testBlockID1)
	defer transaction1.Unpin(testBlockID1)
	transaction1.SetString(testBlockID1, 0, "HOGE1", true)

	transaction2.Pin(testBlockID2)
	// defer transaction2.Unpin(testBlockID2) // のちの transaction2.Commit() で Unpin される.
	transaction2.SetString(testBlockID2, 0, "FUGA2", true)

	transaction3.Pin(testBlockID3)
	defer transaction3.Unpin(testBlockID3)
	transaction3.SetString(testBlockID3, 0, "PIYO3", true)

	// 一部トランザクションは Commit する.
	transaction2.Commit()

	// 完了されていないトランザクションの変更も、一度ディスクに書き込んでおく.
	// BufferManager の pool が溢れるなどで、コミットされていなくてもディスクに書き込まれることはある.
	bufferManager.FlushAll(transaction1.transactionNumber)
	bufferManager.FlushAll(transaction3.transactionNumber)

	// ここで一度システムがクラッシュしたとして、再起動したことをシミュレートする.
	logManager.Flush(9999)
	rebootDatabaseForTransactionTest()

	t.Run("未完了のトランザクションの変更がディスクに書き込まれていること.", func(t *testing.T) {
		page1 := file.NewPage(blockSizeForTransactionTest)
		page3 := file.NewPage(blockSizeForTransactionTest)
		fileManager.Read(testBlockID1, page1)
		fileManager.Read(testBlockID3, page3)

		assert.Equal(t, "HOGE1", page1.GetString(0), "testBlockID1 の変更がディスクに反映されている.")
		assert.Equal(t, "PIYO3", page3.GetString(0), "testBlockID3 の変更がディスクに反映されている.")
	})

	// ここでリブートしたと想定し、Recover を実行する.
	_, _, _, rebootTransaction := startNewTransaction()
	rebootTransaction.Recover()

	t.Run("完了していないトランザクションの変更が巻き戻されていること.", func(t *testing.T) {
		page1 := file.NewPage(blockSizeForTransactionTest)
		page3 := file.NewPage(blockSizeForTransactionTest)
		fileManager.Read(testBlockID1, page1)
		fileManager.Read(testBlockID3, page3)

		assert.Equal(t, "hoge1", page1.GetString(0), "testBlockID1 の変更がディスクに反映されていない.")
		assert.Equal(t, "piyo3", page3.GetString(0), "testBlockID3 の変更がディスクに反映されていない.")
	})

	t.Run("Commitされたトランザクションの変更がディスクに書き込まれていること.", func(t *testing.T) {
		page2 := file.NewPage(blockSizeForTransactionTest)
		fileManager.Read(testBlockID2, page2)

		assert.Equal(t, "FUGA2", page2.GetString(0), "testBlockID2 の変更がディスクに反映されている.")
	})

	t.Run("CHECKPOINT レコードが最後に書き込まれていること.", func(t *testing.T) {
		// NOTE: ディスクに既に書き込まれていることを検証するので、logManager.StreamLogs() は使わない.
		logFileBlockSize := fileManager.GetBlockLength(logFileNameForTransactionTest)
		latestLogBlockID := file.NewBlockID(logFileNameForTransactionTest, logFileBlockSize-1)
		latestLogPage := file.NewPage(blockSizeForTransactionTest)
		fileManager.Read(latestLogBlockID, latestLogPage)

		boundary := latestLogPage.GetInt(0)
		actualCheckpointRecord := log.RawLogRecord(latestLogPage.GetBytes(boundary))

		expectedCheckpointRecordSize := types.Int(4)
		expectedCheckpointLogRecord := file.NewPage(expectedCheckpointRecordSize)
		expectedCheckpointLogRecord.SetInt(0, types.Int(CHECKPOINT))

		assert.Equal(t, log.RawLogRecord(expectedCheckpointLogRecord.Data), actualCheckpointRecord, "CHECKPOINT レコードが最後に書き込まれている.")
	})
}

func TestTransactionSize(t *testing.T) {
	fileManager, _, _, transaction := startNewTransaction()
	_, _, _, transaction2 := startNewTransaction()

	// テスト用の書き換え対象ファイルを準備しておく.
	fileName := "test_transaction_size.data"
	testBlockID1 := file.NewBlockID(fileName, 0)
	testBlockID2 := file.NewBlockID(fileName, 1)
	testPage1 := file.NewPage(blockSizeForTransactionTest)
	testPage2 := file.NewPage(blockSizeForTransactionTest)
	testPage1.SetString(0, "test1")
	testPage2.SetString(0, "test2")
	fileManager.Write(testBlockID1, testPage1)
	fileManager.Write(testBlockID2, testPage2)

	t.Run("ファイルのブロックサイズを複数のトランザクションで正しく取得できる(SLockなので).", func(t *testing.T) {
		result := transaction.Size(fileName)
		result2 := transaction2.Size(fileName)
		fileStat, _ := os.Stat(path.Join(testDirForTransactionTest, fileName))

		assert.Equal(t, types.Int(fileStat.Size()/int64(blockSizeForTransactionTest)), result, "ファイルのサイズが正しく取得できる.")
		assert.Equal(t, types.Int(fileStat.Size()/int64(blockSizeForTransactionTest)), result2, "ファイルのサイズが正しく取得できる.")
	})
}

func TestTransactionAppend(t *testing.T) {
	fileManager, _, _, transaction := startNewTransaction()

	// テスト用の書き換え対象ファイルを準備しておく.
	fileName := "test_transaction_append.data"
	testBlockID1 := file.NewBlockID(fileName, 0)
	testBlockID2 := file.NewBlockID(fileName, 1)
	testPage1 := file.NewPage(blockSizeForTransactionTest)
	testPage2 := file.NewPage(blockSizeForTransactionTest)
	testPage1.SetString(0, "test1")
	testPage2.SetString(0, "test2")
	fileManager.Write(testBlockID1, testPage1)
	fileManager.Write(testBlockID2, testPage2)

	t.Run("正しくブロックの追加が行われる", func(t *testing.T) {
		appendedBlockID := transaction.Append(fileName)
		expectedBlockID := file.NewBlockID(fileName, 2)
		assert.Equal(t, expectedBlockID, appendedBlockID, "新しいブロックが追加される.")
	})

	t.Run("XLock が獲得されているので、他トランザクションからのファイルサイズの取得がブロックされる.", func(t *testing.T) {
		_, _, _, transaction2 := startNewTransaction()
		assert.Panics(t, func() { transaction2.Size(fileName) }, "他トランザクションからのファイルサイズの取得がブロックされる.")
	})

	t.Run("XLock解放後には他トランザクションからファイルサイズの読み取りが可能になる.", func(t *testing.T) {
		_, _, _, transaction3 := startNewTransaction()
		transaction.Commit()
		assert.NotPanics(t, func() { transaction3.Size(fileName) }, "他トランザクションからのファイルサイズの取得が可能になる.")
	})
}
