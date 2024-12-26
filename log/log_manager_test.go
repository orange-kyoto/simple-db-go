package log

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"simple-db-go/file"
	"simple-db-go/util"
	"testing"

	"github.com/google/go-cmp/cmp"
)

const (
	testDir         = "./testlog"
	testLogFileName = "test.log"
	blockSize       = 16
)

func testLogFilePath() string {
	return filepath.Join(testDir, testLogFileName)
}

func cleanLogFile() {
	os.Remove(testLogFilePath())
}

func setup(t *testing.T) *LogManager {
	t.Helper()

	cleanLogFile()
	fileManager := file.NewFileManager(testDir, blockSize)
	logManager := NewLogManager(fileManager, testLogFileName)

	return logManager
}

func cleanup(t *testing.T, lm *LogManager) {
	t.Helper()

	cleanLogFile()
	lm.Close()
}

// ログファイルがない状態でLogManagerを初期化する
// 期待値:
// 1. ログファイルが作成される
// 2. ログファイルに1つだけブロックが追加されており、先頭４バイトにはブロックサイズが書き込まれている
func TestInitLogManagerWithoutLogFile(t *testing.T) {
	logManager := setup(t)
	defer cleanup(t, logManager)

	// ログファイルが作成されているか確認
	logFileInfo, err := os.Stat(testLogFilePath())
	if os.IsNotExist(err) {
		t.Fatalf("ログファイルが作成されていません. log_file=%s", testLogFilePath())
	}
	// ログファイルサイズがブロックサイズと一致しているか確認
	if logFileInfo.Size() != int64(blockSize) {
		t.Fatalf("ログファイルのサイズが不正です. log_file=%s, size=%d, expected=%d",
			testLogFilePath(),
			logFileInfo.Size(),
			blockSize)
	}
	// latestLSN, lastSavedLSN が 0 であることを確認
	if logManager.latestLSN != 0 || logManager.lastSavedLSN != 0 {
		t.Fatalf("latestLSN, lastSavedLSN が不正です. latestLSN=%d, lastSavedLSN=%d", logManager.latestLSN, logManager.lastSavedLSN)
	}
	// Log Manager が保持する currentBlockID, logPage が期待した内容か確認
	expectedBlockID := file.NewBlockID(testLogFileName, 0)
	if !cmp.Equal(logManager.currentBlockID, expectedBlockID) {
		t.Fatalf("currentBlockID が不正です. actual=%v, expected=%v", logManager.currentBlockID, expectedBlockID)
	}
	expectedBytes := make([]byte, blockSize)
	binary.LittleEndian.PutUint32(expectedBytes, uint32(blockSize))
	expectedLogPage := file.NewPageFrom(expectedBytes)
	if !cmp.Equal(logManager.logPage, expectedLogPage) {
		t.Fatalf("logPage が不正です. actual=%v, expected=%v", logManager.logPage, expectedLogPage)
	}

	t.Run("boundary が blockSize に一致すること.", func(t *testing.T) {
		boundary := logManager.logPage.GetInt(0)
		if boundary != blockSize {
			t.Fatalf("boundary が不正です. actual=%d, expected=%d", boundary, blockSize)
		}
	})
}

// ログレコードを追加するテスト. 余白がある場合
func TestAppendLogRecordWithRoom(t *testing.T) {
	logManager := setup(t)
	defer cleanup(t, logManager)

	// LogPage はまだ余白があるので、収まる範囲でログレコードを追加する
	record := []byte("test") // 4 bytes
	lsn := logManager.Append(record)

	t.Run("返された lsn が logManager.latestLSN に一致すること", func(t *testing.T) {
		if lsn != logManager.latestLSN {
			t.Fatalf("lsn が不正です. actual=%d, expected=%d", lsn, logManager.latestLSN)
		}
	})

	t.Run("logManager.lastSavedLSN が更新されていないこと.", func(t *testing.T) {
		if logManager.lastSavedLSN != 0 {
			t.Fatalf("lastSavedLSN が不正です. actual=%d, expected=0", logManager.lastSavedLSN)
		}
	})

	t.Run("logManager.currentBlockIDが先頭ブロックになっていること.", func(t *testing.T) {
		expectedBlockID := file.NewBlockID(testLogFileName, 0)
		if !cmp.Equal(logManager.currentBlockID, expectedBlockID) {
			t.Fatalf("currentBlockID が不正です. actual=%v, expected=%v", logManager.currentBlockID, expectedBlockID)
		}
	})

	t.Run("lm.logPage の先頭４バイトがログレコードのオフセットの位置（boundary）になっていること", func(t *testing.T) {
		// ログレコードは後ろから書き込むので.
		expectedBoundary := blockSize - (file.Int32ByteSize + util.Len(record))
		if boundary := logManager.logPage.GetInt(0); boundary != expectedBoundary {
			t.Fatalf("boundary が不正です. actual=%d, expected=%d, lm=%v", boundary, expectedBoundary, logManager)
		}
	})

	t.Run("lm.logPage のboundary以外のバイト列が書き込んだバイト列に一致していること", func(t *testing.T) {
		// 注意: 末尾にログレコードが書き込まれ、それ以外は0で埋められている
		bytes := make([]byte, blockSize)
		// boundary にはログレコードのオフセットが書き込まれている
		boundary := blockSize - len(record)
		binary.LittleEndian.PutUint32(bytes, uint32(boundary))
		// offset = boundary の位置には、ログレコードが書き込まれている
		// ただし、ログレコードの先頭4バイトは ログレコード自身のバイトサイズである.
		recordBytes := make([]byte, file.Int32ByteSize+util.Len(record))
		binary.LittleEndian.PutUint32(recordBytes, uint32(len(record)))
		copy(recordBytes[file.Int32ByteSize:], record)
		copy(bytes[boundary:], recordBytes)

		if cmp.Equal(logManager.logPage.Data, bytes) {
			t.Fatalf("logPage が不正です. actual=%v, expected=%v", logManager.logPage.Data, bytes)
		}
	})

	t.Run("ディスク上のログファイルは１つだけブロックが追加され、boundaryだけが追加されていること.", func(t *testing.T) {
		fileInfo, err := os.Stat(testLogFilePath())
		if err != nil {
			t.Fatalf("ログファイルの情報取得に失敗しました. %v", err)
		}

		if fileInfo.Size() != blockSize {
			t.Fatalf("ログファイルのサイズが不正です. actual=%d, expected=%d", fileInfo.Size(), blockSize)
		}

		f, err := os.Open(testLogFilePath())
		if err != nil {
			t.Fatalf("ログファイルのオープンに失敗しました. %v", err)
		}
		content := make([]byte, blockSize)
		_, err = f.ReadAt(content, 0)
		if err != nil {
			t.Fatalf("ファイルの読み込みに失敗しました. %v", err)
		}

		boundary := make([]byte, file.Int32ByteSize)
		other := make([]byte, blockSize-file.Int32ByteSize)
		copy(boundary, content[:file.Int32ByteSize])
		copy(other, content[file.Int32ByteSize:])

		expectedBoundary := make([]byte, file.Int32ByteSize)
		binary.LittleEndian.PutUint32(expectedBoundary, uint32(blockSize))

		if !bytes.Equal(boundary, expectedBoundary) {
			t.Fatalf("boundary が不正です. actual=%v, expected=%v", boundary, expectedBoundary)
		}
		if !bytes.Equal(other, make([]byte, blockSize-file.Int32ByteSize)) {
			t.Fatalf("boundary 以外の部分が不正です. actual=%v, expected=%v", other, make([]byte, blockSize-file.Int32ByteSize))
		}
	})
}

// ログレコードを追加するテスト. 余白がない場合
func TestAppendLogRecordWithoutRoom(t *testing.T) {
	logManager := setup(t)
	defer cleanup(t, logManager)

	// LogPage はまだ余白があるので、収まる範囲でログレコードを追加する
	record1 := []byte("test") // 4 bytes
	logManager.Append(record1)

	// 余白に収まらないサイズのレコードを追加する
	record2 := []byte("test2") // 5 bytes
	logManager.Append(record2)

	t.Run("logManager.latestLSN,lastSavedLSNが期待する値になること.", func(t *testing.T) {
		if logManager.latestLSN != 2 {
			t.Fatalf("latestLSN が不正です. actual=%d, expected=2", logManager.latestLSN)
		}
		if logManager.lastSavedLSN != 1 {
			t.Fatalf("lastSavedLSN が不正です. actual=%d, expected=1", logManager.lastSavedLSN)
		}
	})

	t.Run("logManager.logPageは、2つ目のレコードだけが書き込まれていること.", func(t *testing.T) {
		expectedLogPageBytes := make([]byte, blockSize)
		expectedBoundary := blockSize - (file.Int32ByteSize + util.Len(record2))
		expectedRecordBytes := make([]byte, file.Int32ByteSize+util.Len(record2))
		binary.LittleEndian.PutUint32(expectedRecordBytes, uint32(len(record2)))
		copy(expectedRecordBytes[file.Int32ByteSize:], record2)
		binary.LittleEndian.PutUint32(expectedLogPageBytes, uint32(expectedBoundary))
		copy(expectedLogPageBytes[expectedBoundary:], expectedRecordBytes)

		if !bytes.Equal(expectedLogPageBytes, logManager.logPage.Data) {
			t.Fatalf("logPage が不正です. actual=%v, expected=%v", logManager.logPage.Data, expectedLogPageBytes)
		}
	})

	t.Run("logManager.currentBlockIDは2つ目のブロックになっていること.", func(t *testing.T) {
		expectedBlockID := file.NewBlockID(testLogFileName, 1)
		if !cmp.Equal(logManager.currentBlockID, expectedBlockID) {
			t.Fatalf("currentBlockID が不正です. actual=%v, expected=%v", logManager.currentBlockID, expectedBlockID)
		}
	})

	t.Run("ディスク上のログファイルにはブロック1つだけ書き込まれており、record1だけが記録されている.", func(t *testing.T) {
		expectedContent := make([]byte, blockSize)
		expectedBoundary := blockSize - (file.Int32ByteSize + util.Len(record1))
		expectedRecordBytes := make([]byte, file.Int32ByteSize+util.Len(record1))
		binary.LittleEndian.PutUint32(expectedRecordBytes, uint32(len(record1)))
		copy(expectedRecordBytes[file.Int32ByteSize:], record1)
		binary.LittleEndian.PutUint32(expectedContent, uint32(expectedBoundary))
		copy(expectedContent[expectedBoundary:], expectedRecordBytes)

		f, _ := os.Open(testLogFilePath())
		content := make([]byte, blockSize)
		f.Read(content)

		if !bytes.Equal(expectedContent, content) {
			t.Fatalf("ファイルの内容が不正です. actual=%v, expected=%v", content, expectedContent)
		}
	})
}

func TestStreamLogs(t *testing.T) {
	logManager := setup(t)
	defer cleanup(t, logManager)

	// LogPage はまだ余白があるので、収まる範囲でログレコードを追加する
	record1 := []byte("test") // 4 bytes
	logManager.Append(record1)

	// 余白に収まらないサイズのレコードを追加する
	record2 := []byte("test2") // 5 bytes
	logManager.Append(record2)

	// 余白に収まらないサイズのレコードを追加する
	record3 := []byte("test3") // 5 bytes
	logManager.Append(record3)

	// 注意：currentBlockID は 3 になっているので、一番後ろのブロックからスタートすることになる
	logChan := logManager.StreamLogs()

	t.Run("ログレコードが順番に取得できること.", func(t *testing.T) {
		var receivedRecords [][]byte
		for record := range logChan {
			receivedRecords = append(receivedRecords, record)
		}

		if len(receivedRecords) != 3 {
			t.Fatalf("ログレコードの数が不正です. actual=%d, expected=3", len(receivedRecords))
		}

		if !bytes.Equal(receivedRecords[0], record3) {
			t.Fatalf("ログレコード3が不正です. actual=%v, expected=%v", receivedRecords[0], record3)
		}

		if !bytes.Equal(receivedRecords[1], record2) {
			t.Fatalf("ログレコード2が不正です. actual=%v, expected=%v", receivedRecords[1], record2)
		}

		if !bytes.Equal(receivedRecords[2], record1) {
			t.Fatalf("ログレコード1が不正です. actual=%v, expected=%v", receivedRecords[2], record1)
		}
	})
}
