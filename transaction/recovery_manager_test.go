package transaction

import (
	"os"
	"path/filepath"
	"simple-db-go/buffer"
	"simple-db-go/constants"
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/types"
	"simple-db-go/util"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecoveryManagerInitialization(t *testing.T) {
	startNewTransactionForTest(t, recoveryManagerTestName)
	fileManager := file.GetManagerForTest(recoveryManagerTestName)
	logManager := log.GetManagerForTest(recoveryManagerTestName)

	t.Run("RecoveryManager 開始時にログに START レコードが記録される.", func(t *testing.T) {
		latestLSN, lastSavedLSN := logManager.GetLSN()
		assert.Equal(t, log.LSN(1), latestLSN, "まだログは1つだけなので、latestLSN は 1 であるべき")
		assert.Equal(t, log.LSN(0), lastSavedLSN, "まだファイルに書き込まれていないので、lastSavedLSN は 0 であるべき")

		// LogPage の内容の検証
		logPage := logManager.GetLogPage()
		expectedLogRecordSize := constants.Int32ByteSize * 2
		expectedBoundary := types.Int(500) // blockSize - (constants.Int32ByteSize + expectedLogRecordSize)

		assert.Equal(t, expectedBoundary, logPage.GetInt(0), "ログレコードのboundaryは 4 であるべき")
		assert.Equal(t, expectedLogRecordSize, logPage.GetInt(expectedBoundary), "ログレコードのサイズは 8 であるべき")
		assert.Equal(t, types.Int(START), logPage.GetInt(expectedBoundary+constants.Int32ByteSize), "ログレコードの種類は START であるべき")
		assert.Equal(t, types.Int(1), logPage.GetInt(expectedBoundary+constants.Int32ByteSize*2), "トランザクション番号は 1 であるべき")

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
		fileInfo, _ := os.Stat(filepath.Join(recoveryManagerTestName, logFileNameForRMTest))
		expectedBlockID := file.NewBlockID(logFileNameForRMTest, 0)
		expectedFilePage := file.NewPage(blockSize)
		fileManager.Read(expectedBlockID, expectedFilePage)

		assert.Equal(t, int64(blockSize), fileInfo.Size(), "ファイルに書き込まれていないが、logManager が最初のブロック分を append している.")
		assert.Equal(t, types.Int(blockSize), expectedFilePage.GetInt(0), "ファイルに書き込まれていないので、boundary だけが書き込まれている.")
		assert.Equal(t, make([]byte, blockSize-constants.Int32ByteSize), expectedFilePage.Data[constants.Int32ByteSize:], "boundary 以外は空のままである.")
	})
}

func TestRecoveryManagerSetInt(t *testing.T) {
	transaction := startNewTransactionForTest(t, recoveryManagerTestName)
	fileManager := file.GetManagerForTest(recoveryManagerTestName)
	logManager := log.GetManagerForTest(recoveryManagerTestName)
	bufferManager := buffer.GetManagerForTest(recoveryManagerTestName)
	recoveryManager := transaction.recoveryManager

	// テスト用の書き換え対象ファイルを準備しておく.
	oldValue := 79
	testBlockID := file.NewBlockID(dataFileNameForRMTest, 0)
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
		tpos := constants.Int32ByteSize
		assert.Equal(t, types.Int(2), rawRecordPage.GetInt(tpos), "トランザクション番号は 2 であるべき")
		fpos := tpos + constants.Int32ByteSize
		assert.Equal(t, testBlockID.Filename, rawRecordPage.GetString(fpos), "ファイル名は test_recovery_manager.data であるべき")
		bpos := fpos + file.MaxLength(util.Len(testBlockID.Filename))
		assert.Equal(t, testBlockID.BlockNumber, types.BlockNumber(rawRecordPage.GetInt(bpos)), "ブロック番号は 0 であるべき")
		opos := bpos + constants.Int32ByteSize
		assert.Equal(t, types.Int(2), rawRecordPage.GetInt(opos), "オフセットは 2 であるべき")
		vpos := opos + constants.Int32ByteSize
		assert.Equal(t, types.Int(oldValue), rawRecordPage.GetInt(vpos), "古い値は 79 であるべき")

		expectedRecordSize := types.Int(50) // vpos + constants.Int32ByteSize
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
	transaction := startNewTransactionForTest(t, recoveryManagerTestName)
	fileManager := file.GetManagerForTest(recoveryManagerTestName)
	logManager := log.GetManagerForTest(recoveryManagerTestName)
	bufferManager := buffer.GetManagerForTest(recoveryManagerTestName)
	recoveryManager := transaction.recoveryManager

	// テスト用の書き換え対象ファイルを準備しておく.
	oldValue := "orange-kyoto"
	testBlockID := file.NewBlockID(dataFileNameForRMTest, 1)
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
		tpos := constants.Int32ByteSize
		assert.Equal(t, types.Int(3), rawRecordPage.GetInt(tpos), "トランザクション番号は 3 であるべき")
		fpos := tpos + constants.Int32ByteSize
		assert.Equal(t, testBlockID.Filename, rawRecordPage.GetString(fpos), "ファイル名は test_recovery_manager.data であるべき")
		bpos := fpos + file.MaxLength(util.Len(testBlockID.Filename))
		assert.Equal(t, testBlockID.BlockNumber, types.BlockNumber(rawRecordPage.GetInt(bpos)), "ブロック番号は 1 であるべき")
		opos := bpos + constants.Int32ByteSize
		assert.Equal(t, types.Int(3), rawRecordPage.GetInt(opos), "オフセットは 3 であるべき")
		vpos := opos + constants.Int32ByteSize
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

func TestRecoveryManagerCommit(t *testing.T) {
	// まず先に、Commit の仕様を理解するところから始めよう。
	//
	// 1. bufferManager.FlushAll(rm.transactionNumber)
	// 	 - bufferManager にある、該当トランザクションが変更を加えたバッファーをすべてディスクに書き込む.
	// 2. WriteCommitRecord(rm.logManager, rm.transactionNumber)
	// 	 - これはログページにコミットれこーどを書き込むだけ.
	//   - この時点ではまだディスクには書き込まれていないのか.
	// 3. logManager.Flush(lsn)
	// 	- これでログファイルに書き込まれる. COMMIT も含め、もしまだ書き込まれていないログがあっても書き込まれるはず.

	transaction := startNewTransactionForTest(t, recoveryManagerTestName)
	fileManager := file.GetManagerForTest(recoveryManagerTestName)
	bufferManager := buffer.GetManagerForTest(recoveryManagerTestName)
	recoveryManager := transaction.recoveryManager

	// テスト用の書き換え対象ファイルを準備しておく.
	oldValue1 := "hello"
	oldValue2 := "orange"
	oldValue3 := "apple"
	testBlockID1 := file.NewBlockID(dataFileNameForRMTest, 0)
	testBlockID2 := file.NewBlockID(dataFileNameForRMTest, 1)
	testBlockID3 := file.NewBlockID(dataFileNameForRMTest, 2)
	testPage1 := file.NewPage(blockSize)
	testPage2 := file.NewPage(blockSize)
	testPage3 := file.NewPage(blockSize)
	testPage1.SetString(0, oldValue1)
	testPage2.SetString(0, oldValue2)
	testPage3.SetString(0, oldValue3)
	fileManager.Write(testBlockID1, testPage1)
	fileManager.Write(testBlockID2, testPage2)
	fileManager.Write(testBlockID3, testPage3)

	// まず SetString でレコードを書き込んでおき、コミットする.
	newValue1 := "HELLO"
	newValue2 := "ORANGE"
	newValue3 := "BANANA"
	buffer1 := bufferManager.Pin(testBlockID1)
	buffer2 := bufferManager.Pin(testBlockID2)
	buffer3 := bufferManager.Pin(testBlockID3)
	defer bufferManager.Unpin(buffer1)
	defer bufferManager.Unpin(buffer2)
	defer bufferManager.Unpin(buffer3)

	// 先に setup で開始したトランザクションで変更を実施する
	// 注意：recovery manager は先に buffer から古い値を読み取るので、buffer の書き換え前に行うべき.
	lsn1 := recoveryManager.SetString(buffer1, 0, newValue1)
	buffer1.GetContents().SetString(0, newValue1)
	buffer1.SetModified(transaction.transactionNumber, lsn1)
	lsn2 := recoveryManager.SetString(buffer2, 0, newValue2)
	buffer2.GetContents().SetString(0, newValue2)
	buffer2.SetModified(transaction.transactionNumber, lsn2)

	// 別のトランザクションでも書き換えを行う.
	transaction2 := startNewTransactionForTest(t, recoveryManagerTestName)
	lsn3 := transaction2.recoveryManager.SetString(buffer3, 0, newValue3)
	buffer3.GetContents().SetString(0, newValue3)
	buffer3.SetModified(transaction2.transactionNumber, lsn3)

	recoveryManager.Commit() // transaction2 はコミットしない.

	t.Run("commit されたトランザクションが Buffer に対して行なった変更がディスクに書き込まれている.", func(t *testing.T) {
		resultPage1 := file.NewPage(blockSize)
		resultPage2 := file.NewPage(blockSize)
		fileManager.Read(testBlockID1, resultPage1)
		fileManager.Read(testBlockID2, resultPage2)

		assert.Equal(t, "HELLO", resultPage1.GetString(0), "1つ目のブロックに HELLO が書き込まれているべき")
		assert.Equal(t, "ORANGE", resultPage2.GetString(0), "2つ目のブロックに ORANGE が書き込まれているべき")
	})

	t.Run("commit されていないトランザクションが Buffer に対して行なった変更はディスクに書き込まれていない.", func(t *testing.T) {
		resultPage3 := file.NewPage(blockSize)
		fileManager.Read(testBlockID3, resultPage3)
		assert.Equal(t, "apple", resultPage3.GetString(0), "3つ目のブロックにはまだ apple が書き込まれているべき. BANANA にはなっていない.")
	})

	t.Run("1つのSTARTレコード、2つのSETSTRINGレコード、1つのCOMMITレコードがすべてディスクに書き込まれている.", func(t *testing.T) {
		// NOTE: ファイルに書いてあるものを直接見るべきなので、LogManager から取得するのではなく、FileManager から取得する.

		// 念の為、ブロック1つ分だけ書き込まれていることも確認.
		fileInfo, _ := os.Stat(filepath.Join(recoveryManagerTestName, logFileNameForRMTest))
		expectedFileSize := blockSize
		assert.Equal(t, int64(expectedFileSize), fileInfo.Size(), "ファイルに書き込まれているログは1つ分であるべき")

		// 以下の順番でログが書き込まれているはず:
		//
		// START レコード1 : <START 1> (8bytes)
		// SETSTRING レコード1 : <SETSTRING 1 test_recovery_manager.data 0 0 hello>  ((26+4) + (5+4) + 4 * 4 = 55bytes)
		// SETSTRING レコード2 : <SETSTRING 1 test_recovery_manager.data 1 0 orange> ((26+4) + (6+4) + 4 * 4 = 56bytes)
		// START レコード2 : <START 2> (8bytes)
		// SETSTRING レコード3 : <SETSTRING 2 test_recovery_manager.data 2 0 apple>  ((26+4) + (5+4) + 4 * 4 = 55bytes)
		// COMMIT レコード : <COMMIT 1> (8bytes)
		//
		// 各バイト列の先頭に、そのバイト列の大きさが4bytes記録されていることに注意.
		// さらにログファイルの各ブロックの先頭4bytesには boundary が記録されていることにも注意.

		testLogBlockID := file.NewBlockID(logFileNameForRMTest, 0)
		testLogPage := file.NewPage(blockSize)
		fileManager.Read(testLogBlockID, testLogPage)

		// 注意：バイトサイズ記録用の4バイトを追加している.
		startRecord1ByteSize := 4 + 8
		setStringRecord1ByteSize := 4 + 55
		setStringRecord2ByteSize := 4 + 56
		startRecord2ByteSize := 4 + 8
		setStringRecord3ByteSize := 4 + 55
		commitRecordByteSize := 4 + 8

		// 注意: このテストの前からログファイルに書き込まれているものがあるので、その分も考慮する必要がある. これちょっと辛いな...
		expectedBoundary := blockSize -
			// 1つ目のテスト. START レコードのみ.
			(4 + 8) -
			// 2つ目のテスト. START x 1, SETINT x 1
			((4 + 8) + (4 + 50)) -
			// 3つ目のテスト. START x 1, SETSTRING x 1
			((4 + 8) + (4 + 62)) -
			// このテストで書き込まれると期待されるレコードのバイトサイズの合計
			(startRecord1ByteSize +
				setStringRecord1ByteSize +
				setStringRecord2ByteSize +
				startRecord2ByteSize +
				setStringRecord3ByteSize +
				commitRecordByteSize)

		assert.Equal(t, types.Int(expectedBoundary), testLogPage.GetInt(0), "ログレコードのboundaryが期待された値でない.")

		// Commit レコードの検証はバイト列まで丁寧に検証する.
		commitRecordBytes := testLogPage.GetBytes(types.Int(expectedBoundary))
		commitRecordPage := file.NewPageFrom(commitRecordBytes)
		commitRecord := NewCommitRecord(commitRecordPage)
		assert.Equal(t, types.Int(COMMIT), commitRecordPage.GetInt(0), "COMMIT レコードであるべき")
		assert.Equal(t, types.Int(4), commitRecordPage.GetInt(constants.Int32ByteSize), "COMMIT レコードのトランザクション番号は 4 であるべき")
		assert.Equal(t, types.TransactionNumber(4), commitRecord.GetTransactionNumber(), "COMMIT レコードのトランザクション番号は 4 であるべき")

		// 他のレコードが書き込まれていることも簡易的に確認する. バイト列自体の検証はしない.

		setStringRecord3Offset := expectedBoundary + commitRecordByteSize
		setStringRecord3Bytes := testLogPage.GetBytes(types.Int(setStringRecord3Offset))
		setStringRecord3 := NewSetStringRecord(file.NewPageFrom(setStringRecord3Bytes))

		startRecord2Offset := setStringRecord3Offset + setStringRecord3ByteSize
		startRecord2Bytes := testLogPage.GetBytes(types.Int(startRecord2Offset))
		startRecord2 := NewStartRecord(file.NewPageFrom(startRecord2Bytes))

		setStringRecord2Offset := startRecord2Offset + startRecord2ByteSize
		setStringRecord2Bytes := testLogPage.GetBytes(types.Int(setStringRecord2Offset))
		setStringRecord2 := NewSetStringRecord(file.NewPageFrom(setStringRecord2Bytes))

		setStringRecord1Offset := setStringRecord2Offset + setStringRecord2ByteSize
		setStringRecord1Bytes := testLogPage.GetBytes(types.Int(setStringRecord1Offset))
		setStringRecord1 := NewSetStringRecord(file.NewPageFrom(setStringRecord1Bytes))

		startRecordOffset := setStringRecord1Offset + setStringRecord1ByteSize
		startRecordBytes := testLogPage.GetBytes(types.Int(startRecordOffset))
		startRecord := NewStartRecord(file.NewPageFrom(startRecordBytes))

		expectedSetStringRecord1 := &SetStringRecord{
			transactionNumber: 4,
			offset:            0,
			oldValue:          "hello",
			blockID:           testBlockID1,
		}
		expectedSetStringRecord2 := &SetStringRecord{
			transactionNumber: 4,
			offset:            0,
			oldValue:          "orange",
			blockID:           testBlockID2,
		}
		expectedSetStringRecord3 := &SetStringRecord{
			transactionNumber: 5,
			offset:            0,
			oldValue:          "apple",
			blockID:           testBlockID3,
		}
		expectedStartRecord1 := &StartRecord{
			transactionNumber: 4,
		}
		expectedStartRecord2 := &StartRecord{
			transactionNumber: 5,
		}

		assert.Equal(t, expectedSetStringRecord1, setStringRecord1, "1つ目のSETSTRINGレコードが正しく書き込まれているべき")
		assert.Equal(t, expectedSetStringRecord2, setStringRecord2, "2つ目のSETSTRINGレコードが正しく書き込まれているべき")
		assert.Equal(t, expectedSetStringRecord3, setStringRecord3, "3つ目のSETSTRINGレコードが正しく書き込まれているべき")
		assert.Equal(t, expectedStartRecord1, startRecord, "STARTレコードが正しく書き込まれているべき")
		assert.Equal(t, expectedStartRecord2, startRecord2, "STARTレコードが正しく書き込まれているべき")
	})
}

// NOTE: Rollback, Recover は Transaction のメソッドに依存しているので、Transaction のテストで行う.
