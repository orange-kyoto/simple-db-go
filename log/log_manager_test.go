package log

import (
	"os"
	"path"
	"simple-db-go/constants"
	"simple-db-go/file"
	"simple-db-go/types"
	"simple-db-go/util"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitLogManagerWithoutLogFile(t *testing.T) {
	logManager := getLogManagerForTest(t)

	t.Run("ログファイルが期待したファイルサイズで存在すること.", func(t *testing.T) {
		logFileInfo, err := os.Stat(path.Join(logManagerTestName, logManagerTestName+".log"))
		assert.True(t, err == nil, "ログファイルが作成されていること")
		assert.Equal(t, int64(blockSize), logFileInfo.Size(), "ログファイルのサイズがブロックサイズと一致していること")
	})

	t.Run("LSN が正しく初期化されていること.", func(t *testing.T) {
		assert.Equal(t, LSN(0), logManager.latestLSN, "latestLSN が 0 であること")
		assert.Equal(t, LSN(0), logManager.lastSavedLSN, "lastSavedLSN が 0 であること")
	})

	t.Run("currentBlockID が正しく初期化されていること.", func(t *testing.T) {
		expectedBlockID := file.NewBlockID(logManagerTestName+".log", 0)
		assert.Equal(t, expectedBlockID, logManager.currentBlockID, "currentBlockID が 0 であること")
	})

	t.Run("logPage が正しく初期化されていること.", func(t *testing.T) {
		expectedPage := file.NewPage(blockSize)
		initialBoundary := blockSize
		expectedPage.SetInt(0, types.Int(initialBoundary))
		assert.Equal(t, expectedPage, logManager.logPage, "logPage が正しく初期化されていること")
	})
}

// ログレコードを追加するテスト. 余白がある場合
func TestAppendLogRecordWithRoom(t *testing.T) {
	logManager := getLogManagerForTest(t)

	// LogPage はまだ余白があるので、収まる範囲でログレコードを追加する
	testRecordBytes := []byte("test") // 4 bytes
	lsn := logManager.Append(testRecordBytes)

	t.Run("返された lsn が logManager.latestLSN に一致すること", func(t *testing.T) {
		assert.Equal(t, lsn, logManager.latestLSN, "lsn が latestLSN に一致すること")
	})

	t.Run("logManager.lastSavedLSN が更新されていないこと.", func(t *testing.T) {
		assert.Equal(t, LSN(0), logManager.lastSavedLSN, "lastSavedLSN が 0 のままであること")
	})

	t.Run("logManager.currentBlockIDが先頭ブロックになっていること.", func(t *testing.T) {
		expectedBlockID := file.NewBlockID(logManagerTestName+".log", 0)
		assert.Equal(t, expectedBlockID, logManager.currentBlockID, "currentBlockID が 0 であること")
	})

	t.Run("lm.logPage の先頭４バイトがログレコードのオフセットの位置（boundary）になっていること", func(t *testing.T) {
		// ログレコードは後ろから書き込むので.
		expectedBoundary := blockSize - (constants.Int32ByteSize + util.Len(testRecordBytes))
		assert.Equal(t, expectedBoundary, logManager.logPage.GetInt(0), "boundary が正しく設定されていること")
	})

	t.Run("lm.logPage のboundary以外のバイト列が書き込んだバイト列に一致していること", func(t *testing.T) {
		expectedPage := file.NewPage(blockSize)
		expectedBoundary := blockSize - (constants.Int32ByteSize + util.Len(testRecordBytes)) // バイト列の長さが先頭に付与されるので.
		expectedPage.SetInt(0, types.Int(expectedBoundary))
		expectedPage.SetBytes(types.Int(expectedBoundary), testRecordBytes)

		assert.Equal(t, expectedPage, logManager.logPage, "logPage が正しく設定されていること")
	})

	t.Run("ディスク上のログファイルは初期化時のままで、追加されたログはまだディスクに書き込まれていないこと.", func(t *testing.T) {
		fileInfo, _ := os.Stat(path.Join(logManagerTestName, logManagerTestName+".log"))
		assert.Equal(t, int64(blockSize), fileInfo.Size(), "ログファイルのサイズがブロックサイズと一致していること")

		logBlockID := file.NewBlockID(logManagerTestName+".log", 0)

		expectedWrittenLogPage := file.NewPage(blockSize)
		expectedWrittenLogPage.SetInt(0, blockSize)

		writtenLogPage := file.NewPage(blockSize)
		logManager.fileManager.Read(logBlockID, writtenLogPage)

		assert.Equal(t, expectedWrittenLogPage, writtenLogPage, "ディスク上のログファイルには書き込まれていないこと")
	})
}

// ログレコードを追加するテスト. 余白がない場合
func TestAppendLogRecordWithoutRoom(t *testing.T) {
	// 注意：前のテストで、すでに1つレコードが追加されていることに注意。まだ LogPage にあるだけで、ディスクには書き込まれていない.
	logManager := getLogManagerForTest(t)

	record1 := []byte("test")  // 4 bytes これがすでに書き込まれている.
	record2 := []byte("test2") // 5 bytes こちらはまだ書き込まれていない.
	logManager.Append(record2) // 余白が足りないので、新しいブロックに書き込むことが期待される.

	t.Run("logManager.latestLSN,lastSavedLSNが期待する値になること.", func(t *testing.T) {
		assert.Equal(t, LSN(2), logManager.latestLSN, "latestLSN が 2 になること")
		assert.Equal(t, LSN(1), logManager.lastSavedLSN, "lastSavedLSN が 1 になること")
	})

	t.Run("logManager.logPageは、2つ目のレコードだけが書き込まれていること.", func(t *testing.T) {
		expectedLogPage := file.NewPage(blockSize)
		expectedBoundary := blockSize - (constants.Int32ByteSize + util.Len(record2))
		expectedLogPage.SetInt(0, expectedBoundary)
		expectedLogPage.SetBytes(expectedBoundary, record2)

		assert.Equal(t, expectedLogPage, logManager.logPage, "logPage が正しく設定されていること")
	})

	t.Run("logManager.currentBlockIDは2つ目のブロックになっていること.", func(t *testing.T) {
		expectedBlockID := file.NewBlockID(logManagerTestName+".log", 1)
		assert.Equal(t, expectedBlockID, logManager.currentBlockID, "currentBlockID が 1 になること")
	})

	t.Run("ディスク上のログファイルにはブロック1つだけ書き込まれており、record1だけが記録されている.", func(t *testing.T) {
		expectedWrittenLogPage := file.NewPage(blockSize)
		expectedBoundary := blockSize - (constants.Int32ByteSize + util.Len(record1))
		expectedWrittenLogPage.SetInt(0, expectedBoundary)
		expectedWrittenLogPage.SetBytes(expectedBoundary, record1)

		blockID := file.NewBlockID(logManagerTestName+".log", 0) // 先頭ブロックに書き込まれているはず.
		expectedWrittenLogPage = file.NewPage(blockSize)
		logManager.fileManager.Read(blockID, expectedWrittenLogPage)

		assert.Equal(t, expectedWrittenLogPage, expectedWrittenLogPage, "ディスク上のログファイルにはrecord1だけが書き込まれていること.")
	})
}

func TestStreamLogs(t *testing.T) {
	// 注意：前のテストで、すでに1つレコードが追加されていることに注意。まだ LogPage にあるだけで、ディスクには書き込まれていない.
	logManager := getLogManagerForTest(t)

	record1 := []byte("test")  // 4 bytes これがすでに書き込まれている.
	record2 := []byte("test2") // 5 bytes これもすでに書き込まれている.
	record3 := []byte("test3") // 5 bytes これがまだ書き込まれていない.
	logManager.Append(record3)

	// 注意：currentBlockID は 3 になっているので、一番後ろのブロックからスタートすることになる
	logChan := logManager.StreamLogs()

	receivedRecords := make([]RawLogRecord, 0, 3)
	for record := range logChan {
		receivedRecords = append(receivedRecords, record)
	}

	assert.Equal(t, 3, len(receivedRecords), "ログレコードの数が3であること")
	assert.Equal(t, RawLogRecord(record3), receivedRecords[0], "最新のログレコードが正しいこと")
	assert.Equal(t, RawLogRecord(record2), receivedRecords[1], "２番目のログレコードが正しいこと")
	assert.Equal(t, RawLogRecord(record1), receivedRecords[2], "最古のログレコードが正しいこと")
}
