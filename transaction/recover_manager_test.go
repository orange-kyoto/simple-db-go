package transaction

import (
	"os"
	"path/filepath"
	"simple-db-go/buffer"
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/types"
	"simple-db-go/util"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	testDir        = "test_recovery_manager"
	logFileName    = "test_recovery_manager.log"
	dataFileName   = "test_recovery_manager.data"
	blockSize      = 128
	bufferPoolSize = 3
)

func setup() (*file.FileManager, *log.LogManager, *buffer.BufferManager, *Transaction) {
	cleanup()
	fileManager := file.NewFileManager(testDir, blockSize)
	logManager := log.NewLogManager(fileManager, logFileName)
	bufferManager := buffer.NewBufferManager(fileManager, logManager, bufferPoolSize)
	transaction := NewTransaction(fileManager, logManager, bufferManager)

	return fileManager, logManager, bufferManager, transaction
}

func cleanup() {
	os.RemoveAll(testDir)
}

func TestRecoveryManagerInitialization(t *testing.T) {
	defer cleanup()
	// 注意：Transaction の初期化時に RecoveryManager が初期化される.
	fileManager, logManager, _, _ := setup()

	t.Run("RecoveryManager 開始時にログに START レコードが記録される.", func(t *testing.T) {
		latestLSN, lastSavedLSN := logManager.GetLSN()
		assert.Equal(t, log.LSN(1), latestLSN, "まだログは1つだけなので、latestLSN は 1 であるべき")
		assert.Equal(t, log.LSN(0), lastSavedLSN, "まだファイルに書き込まれていないので、lastSavedLSN は 0 であるべき")

		// LogPage の内容の検証
		logPage := logManager.GetLogPage()
		expectedLogRecordSize := file.Int32ByteSize * 2
		expectedBoundary := types.Int(116) // blockSize - (file.Int32ByteSize + expectedLogRecordSize)

		assert.Equal(t, expectedBoundary, logPage.GetInt(0), "ログレコードのboundaryは 4 であるべき")
		assert.Equal(t, expectedLogRecordSize, logPage.GetInt(expectedBoundary), "ログレコードのサイズは 8 であるべき")
		assert.Equal(t, types.Int(START), logPage.GetInt(expectedBoundary+file.Int32ByteSize), "ログレコードの種類は START であるべき")
		assert.Equal(t, types.Int(1), logPage.GetInt(expectedBoundary+file.Int32ByteSize*2), "トランザクション番号は 1 であるべき")

		// constructor の検証
		rawRecordPage := file.NewPageFrom(logPage.Data[expectedBoundary:])
		startRecord := NewStartRecord(rawRecordPage)
		expectedStartRecord := &StartRecord{
			transactionNumber: 1,
		}
		assert.Equal(t, expectedStartRecord, startRecord, "StartRecord が正しく生成されているべき")

		// ファイルに書き込まれていないことを念の為確認する（理解の確認も含めて）
		// 補足：まだ LogPage に書き込まれただけで、Flush していないから.
		// 補足：ただし、LogManager の初期化時に boundary だけ記録されている.
		fileInfo, _ := os.Stat(filepath.Join(testDir, logFileName))
		expectedBlockID := file.NewBlockID(logFileName, 0)
		expectedFilePage := file.NewPage(blockSize)
		fileManager.Read(expectedBlockID, expectedFilePage)

		assert.Equal(t, int64(blockSize), fileInfo.Size(), "ファイルに書き込まれていないが、logManager が最初のブロック分を append している.")
		assert.Equal(t, types.Int(blockSize), expectedFilePage.GetInt(0), "ファイルに書き込まれていないので、boundary だけが書き込まれている.")
		assert.Equal(t, make([]byte, blockSize-file.Int32ByteSize), expectedFilePage.Data[file.Int32ByteSize:], "boundary 以外は空のままである.")
	})
}

func TestRecoveryManagerSetInt(t *testing.T) {
	defer cleanup()
	fileManager, logManager, bufferManager, transaction := setup()
	recoveryManager := transaction.recoveryManager

	// テスト用の書き換え対象ファイルを準備しておく.
	oldValue := 79
	testBlockID := file.NewBlockID(dataFileName, 0)
	testPage := file.NewPage(blockSize)
	testPage.SetInt(2, types.Int(oldValue))
	fileManager.Write(testBlockID, testPage)

	t.Run("SetInt でログが書き込まれる", func(t *testing.T) {
		buffer := bufferManager.Pin(testBlockID)
		defer bufferManager.Unpin(buffer)
		recoveryManager.SetInt(buffer, 2, 80)

		// LogPage の検証（最新のログは末尾に追加されていると期待して良い）
		boundary := logManager.GetLogPage().GetInt(0)
		rawRecord := logManager.GetLogPage().GetBytes(boundary)
		rawRecordPage := file.NewPageFrom(rawRecord)

		assert.Equal(t, types.Int(SETINT), rawRecordPage.GetInt(0), "ログレコードの種類は SETINT であるべき")
		tpos := file.Int32ByteSize
		assert.Equal(t, types.Int(2), rawRecordPage.GetInt(tpos), "トランザクション番号は 2 であるべき")
		fpos := tpos + file.Int32ByteSize
		assert.Equal(t, testBlockID.Filename, rawRecordPage.GetString(fpos), "ファイル名は test_recovery_manager.data であるべき")
		bpos := fpos + file.MaxLength(util.Len(testBlockID.Filename))
		assert.Equal(t, testBlockID.Blknum, rawRecordPage.GetInt(bpos), "ブロック番号は 0 であるべき")
		opos := bpos + file.Int32ByteSize
		assert.Equal(t, types.Int(2), rawRecordPage.GetInt(opos), "オフセットは 2 であるべき")
		vpos := opos + file.Int32ByteSize
		assert.Equal(t, types.Int(oldValue), rawRecordPage.GetInt(vpos), "古い値は 79 であるべき")

		expectedRecordSize := types.Int(50) // vpos + file.Int32ByteSize
		assert.Equal(t, expectedRecordSize, util.Len(rawRecord), "ログレコードのサイズは 50 であるべき")

		// constructor の検証
		setIntRecord := NewSetIntRecord(rawRecordPage)
		expectedSetIntRecord := &SetIntRecord{
			transactionNumber: 2,
			offset:            2,
			oldValue:          79,
			blockID:           testBlockID,
		}
		assert.Equal(t, expectedSetIntRecord, setIntRecord, "SetIntRecord が正しく生成されているべき")
	})
}

func TestRecoveryManagerSetString(t *testing.T) {
	defer cleanup()
	fileManager, logManager, bufferManager, transaction := setup()
	recoveryManager := transaction.recoveryManager

	// テスト用の書き換え対象ファイルを準備しておく.
	oldValue := "orange-kyoto"
	testBlockID := file.NewBlockID(dataFileName, 1)
	testPage := file.NewPage(blockSize)
	testPage.SetString(3, oldValue)
	fileManager.Write(testBlockID, testPage)

	t.Run("SetString でログが書き込まれる", func(t *testing.T) {
		buffer := bufferManager.Pin(testBlockID)
		defer bufferManager.Unpin(buffer)
		recoveryManager.SetString(buffer, 3, "orange-kyoto-new")

		// LogPage の検証（最新のログは末尾に追加されていると期待して良い）
		boundary := logManager.GetLogPage().GetInt(0)
		rawRecord := logManager.GetLogPage().GetBytes(boundary)
		rawRecordPage := file.NewPageFrom(rawRecord)

		assert.Equal(t, types.Int(SETSTRING), rawRecordPage.GetInt(0), "ログレコードの種類は SETSTRING であるべき")
		tpos := file.Int32ByteSize
		assert.Equal(t, types.Int(3), rawRecordPage.GetInt(tpos), "トランザクション番号は 3 であるべき")
		fpos := tpos + file.Int32ByteSize
		assert.Equal(t, testBlockID.Filename, rawRecordPage.GetString(fpos), "ファイル名は test_recovery_manager.data であるべき")
		bpos := fpos + file.MaxLength(util.Len(testBlockID.Filename))
		assert.Equal(t, testBlockID.Blknum, rawRecordPage.GetInt(bpos), "ブロック番号は 1 であるべき")
		opos := bpos + file.Int32ByteSize
		assert.Equal(t, types.Int(3), rawRecordPage.GetInt(opos), "オフセットは 3 であるべき")
		vpos := opos + file.Int32ByteSize
		assert.Equal(t, oldValue, rawRecordPage.GetString(vpos), "古い値は orange-kyoto であるべき")

		expectedRecordSize := types.Int(62) // 26(test_recovery_manager.data) + 4 + 12(orange-kyoto) + 4 + 4 * 4 = 62
		assert.Equal(t, expectedRecordSize, util.Len(rawRecord), "ログレコードのサイズは 54 であるべき")

		// constructor の検証
		setStringRecord := NewSetStringRecord(rawRecordPage)
		expectedSetStringRecord := &SetStringRecord{
			transactionNumber: 3,
			offset:            3,
			oldValue:          "orange-kyoto",
			blockID:           testBlockID,
		}
		assert.Equal(t, expectedSetStringRecord, setStringRecord, "SetStringRecord が正しく生成されているべき")
	})
}

// TODO: Commit, Rollback, Recovery のテスト追加
