package query_test

import (
	"os"
	"path"
	"simple-db-go/file"
	"simple-db-go/query"
	"simple-db-go/record"
	"simple-db-go/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func buildTestTableSchema() *record.Schema {
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 10)
	schema.AddIntField("age")
	return schema
}

func TestTableScanInitialization(t *testing.T) {
	schema := buildTestTableSchema()
	layout := record.NewLayout(schema)

	t.Run("テーブルファイルが空の場合、ブロックが追加される. そのブロックが current block になる.", func(t *testing.T) {
		tableName := types.TableName("test_table_scan_initialization_1")
		fileName := string(tableName) + ".table"

		// 事前に cleanup しているのでファイルは無い.
		assert.NoFileExists(t, path.Join(tableScanTestName, fileName), "table file should not exist.")

		transaction := newTransactionForTest(t, tableScanTestName)
		tableScan := query.NewTableScan(transaction, tableName, layout)

		fileInfo, _ := os.Stat(path.Join(tableScanTestName, fileName))

		expectedFileByteSize := int64(512) // block size  と同じ.
		assert.Equal(t, expectedFileByteSize, fileInfo.Size(), "file size should be 512 bytes.")

		expectedBlockID := file.NewBlockID(fileName, 0)
		expectedSlotNumber := record.NULL_SLOT_NUMBER
		expectedRecordID := record.NewRecordID(expectedBlockID.BlockNumber, expectedSlotNumber)
		assert.Equal(t, expectedRecordID, tableScan.GetCurrentRecordID(), "current record id should be recordID0.")
	})

	t.Run("既にテーブルファイルが存在する場合、そのファイルの先頭ブロックが current block になる.", func(t *testing.T) {
		tableName := types.TableName("test_table_scan_initialization_2")
		fileName := string(tableName) + ".table"

		// 先にファイルを用意する
		fileManager := file.GetManagerForTest(tableScanTestName)
		testBlockIDs := file.PrepareBlockIDs(2, fileName)
		testPages := file.PreparePages(2, fileManager.BlockSize())
		testPages[0].SetString(0, "hoge")
		testPages[1].SetString(0, "fuga")
		fileManager.Write(testBlockIDs[0], testPages[0])
		fileManager.Write(testBlockIDs[1], testPages[1])
		assert.Equal(t, types.Int(2), fileManager.GetBlockLength(fileName), "file should have 2 blocks.")

		transaction := newTransactionForTest(t, tableScanTestName)
		query.NewTableScan(transaction, tableName, layout)

		assert.Equal(t, types.Int(2), fileManager.GetBlockLength(fileName), "TableScan 初期化後もブロックサイズは2のまま.")
	})
}

func TestTableScanClose(t *testing.T) {
	schema := buildTestTableSchema()
	layout := record.NewLayout(schema)

	t.Run("Close実行後に、RecordPage に読み込んでいたブロックIDに対応するバッファーがUnpinされる.", func(t *testing.T) {
		transaction := newTransactionForTest(t, tableScanTestName)
		tableName := types.TableName("test_table_scan_close")
		tableScan := query.NewTableScan(transaction, tableName, layout)

		assert.Equal(t, types.Int(7), transaction.AvailableBuffers(), "前のテストを含め、合計3つPinされているので利用可能なバッファーは7.")

		tableScan.Close()

		assert.Equal(t, types.Int(8), transaction.AvailableBuffers(), "Close後はUnpinされているので利用可能なバッファーは8.")
	})
}

func TestTableScanMoveToRecordID(t *testing.T) {
	schema := buildTestTableSchema()
	layout := record.NewLayout(schema)

	tableName := types.TableName("test_table_scan_move_to_record_id")
	fileName := string(tableName) + ".table"

	// 先にファイルを用意する
	fileManager := file.GetManagerForTest(tableScanTestName)
	testBlocks := file.PrepareBlockIDs(3, fileName)
	testPages := file.PreparePages(3, fileManager.BlockSize())
	testPages[0].SetString(0, "hoge")
	testPages[1].SetString(0, "fuga")
	testPages[2].SetString(0, "piyo")
	fileManager.Write(testBlocks[0], testPages[0])
	fileManager.Write(testBlocks[1], testPages[1])
	fileManager.Write(testBlocks[2], testPages[2])
	assert.Equal(t, types.Int(3), fileManager.GetBlockLength(fileName), "file should have 3 blocks.")

	transaction := newTransactionForTest(t, tableScanTestName)
	tableScan := query.NewTableScan(transaction, tableName, layout)

	t.Run("指定した record id に移動できる.", func(t *testing.T) {
		recordID1 := record.NewRecordID(types.BlockNumber(1), record.SlotNumber(2))
		tableScan.MoveToRecordID(recordID1)

		assert.Equal(t, recordID1, tableScan.GetCurrentRecordID(), "current record id は recordID1 であるべし.")

		recordID2 := record.NewRecordID(types.BlockNumber(2), record.SlotNumber(5))
		tableScan.MoveToRecordID(recordID2)

		assert.Equal(t, recordID2, tableScan.GetCurrentRecordID(), "current record id は recordID2 であるべし.")
	})

	t.Run("ブロックを移動して値の読み書きができる.", func(t *testing.T) {
		recordID1 := record.NewRecordID(types.BlockNumber(1), record.SlotNumber(2))
		recordID2 := record.NewRecordID(types.BlockNumber(2), record.SlotNumber(5))

		tableScan.MoveToRecordID(recordID1)

		tableScan.SetInt("id", 123)
		tableScan.SetString("name", "orange")
		tableScan.SetInt("age", 12)

		assert.Equal(t, recordID1, tableScan.GetCurrentRecordID(), "current record id は recordID1 であるべし.")
		idValue, idError := tableScan.GetInt("id")
		nameValue, nameError := tableScan.GetString("name")
		ageValue, ageError := tableScan.GetInt("age")
		if assert.NoError(t, idError) && assert.NoError(t, nameError) && assert.NoError(t, ageError) {
			assert.Equal(t, types.Int(123), idValue, "id は 123 であるべし.")
			assert.Equal(t, "orange", nameValue, "name は orange であるべし.")
			assert.Equal(t, types.Int(12), ageValue, "age は 12 であるべし.")
		}

		// 一度 recordID2 に移動して再度戻った後も値が読めることを確認.
		tableScan.MoveToRecordID(recordID2)
		tableScan.MoveToRecordID(recordID1)
		assert.Equal(t, recordID1, tableScan.GetCurrentRecordID(), "current record id は recordID1 であるべし.(移動後)")
		idValue, idError = tableScan.GetInt("id")
		nameValue, nameError = tableScan.GetString("name")
		ageValue, ageError = tableScan.GetInt("age")
		if assert.NoError(t, idError) && assert.NoError(t, nameError) && assert.NoError(t, ageError) {
			assert.Equal(t, types.Int(123), idValue, "id は 123 であるべし.(移動後)")
			assert.Equal(t, "orange", nameValue, "name は orange であるべし.(移動後)")
			assert.Equal(t, types.Int(12), ageValue, "age は 12 であるべし.(移動後)")
		}

		// 一度 commit し、別のトランザクションから読めることを確認.
		transaction.Commit()
		transaction2 := newTransactionForTest(t, tableScanTestName)
		tableScan2 := query.NewTableScan(transaction2, tableName, layout)
		tableScan2.MoveToRecordID(recordID1)

		assert.Equal(t, recordID1, tableScan2.GetCurrentRecordID(), "current record id は recordID1 であるべし.(別トランザクション)")
		idValue2, idError2 := tableScan2.GetInt("id")
		nameValue2, nameError2 := tableScan2.GetString("name")
		ageValue2, ageError2 := tableScan2.GetInt("age")
		if assert.NoError(t, idError2) && assert.NoError(t, nameError2) && assert.NoError(t, ageError2) {
			assert.Equal(t, types.Int(123), idValue2, "id は 123 であるべし.(別トランザクション)")
			assert.Equal(t, "orange", nameValue2, "name は orange であるべし.(別トランザクション)")
			assert.Equal(t, types.Int(12), ageValue2, "age は 12 であるべし.(別トランザクション)")
		}
	})
}

func TestTableScanBeforeFirst(t *testing.T) {
	schema := buildTestTableSchema()
	layout := record.NewLayout(schema)

	tableName := types.TableName("test_table_scan_before_first")
	fileName := string(tableName) + ".table"

	transaction := newTransactionForTest(t, tableScanTestName)
	tableScan := query.NewTableScan(transaction, tableName, layout)
	// ファイルのブロックを2つまで増やしておく.
	transaction.Append(fileName)

	t.Run("先頭レコードの直前に移動できる.", func(t *testing.T) {
		tableScan.MoveToRecordID(record.NewRecordID(types.BlockNumber(1), record.SlotNumber(3)))
		tableScan.BeforeFirst()
		assert.Equal(t, record.NewRecordID(types.BlockNumber(0), record.SlotNumber(-1)), tableScan.GetCurrentRecordID(), "先頭レコードの直前に移動している")
	})
}

func TestTableScanInsert(t *testing.T) {
	schema := buildTestTableSchema()
	layout := record.NewLayout(schema)

	tableName := types.TableName("test_table_scan_insert")
	fileName := string(tableName) + ".table"

	transaction := newTransactionForTest(t, tableScanTestName)
	tableScan := query.NewTableScan(transaction, tableName, layout)
	// ファイルのブロックを2つまで増やしておく.
	transaction.Append(fileName)

	t.Run("最初の Insert 呼び出し時は先頭ブロックの先頭スロットが空いているので、先頭レコードへの書き込みができる状態になる.", func(t *testing.T) {
		tableScan.Insert()
		assert.Equal(t, record.NewRecordID(types.BlockNumber(0), record.SlotNumber(0)), tableScan.GetCurrentRecordID(), "current record id は recordID0 であるべし.")
	})

	t.Run("2回目の Insert 呼び出し後も、まだブロックの移動は行われず、スロット番号のみがインクリメントされる.", func(t *testing.T) {
		tableScan.Insert()
		assert.Equal(t, record.NewRecordID(types.BlockNumber(0), record.SlotNumber(1)), tableScan.GetCurrentRecordID(), "current record id は recordID1 であるべし.")
	})

	t.Run("スロットは19個存在するため、Insert 呼び出しの3回目〜19回目の呼び出しまでは、ブロック番号は変わらず、スロット番号だけがインクリメントされる.", func(t *testing.T) {
		for i := types.Int(2); i < 19; i++ {
			tableScan.Insert()
			assert.Equal(t, record.NewRecordID(types.BlockNumber(0), record.SlotNumber(i)), tableScan.GetCurrentRecordID(), "current record id は recordID1 であるべし.")
		}
	})

	t.Run("次の Insert 呼び出し時にはブロックの移動が行われるため、ブロック番号がインクリメントされ、スロット番号は最初の番号になる.", func(t *testing.T) {
		tableScan.Insert()
		assert.Equal(t, record.NewRecordID(types.BlockNumber(1), record.SlotNumber(0)), tableScan.GetCurrentRecordID(), "current record id は recordID1 であるべし.")
		assert.Equal(t, types.Int(2), transaction.Size(fileName), "既存ブロックへの移動なので、ファイルのブロックサイズは変わっていない.")
	})

	t.Run("もう一度18回 Insert を実行しても、ブロック番号は変わらない.", func(t *testing.T) {
		for i := record.SlotNumber(1); i < 19; i++ {
			tableScan.Insert()
			assert.Equal(t, record.NewRecordID(types.BlockNumber(1), i), tableScan.GetCurrentRecordID(), "current record id は recordID1 であるべし.")
		}
	})

	t.Run("次の Insert 呼び出し時には、ファイルにブロックが1つ追加される.", func(t *testing.T) {
		tableScan.Insert()
		assert.Equal(t, record.NewRecordID(types.BlockNumber(2), record.SlotNumber(0)), tableScan.GetCurrentRecordID(), "current record id は recordID1 であるべし.")
		assert.Equal(t, types.Int(3), transaction.Size(fileName), "新しいブロックが追加されたので、ファイルのブロックサイズは3.")
	})
}

func TestTableScanDelete(t *testing.T) {
	schema := buildTestTableSchema()
	layout := record.NewLayout(schema)

	tableName := types.TableName("test_table_scan_delete")
	fileName := string(tableName) + ".table"

	transaction := newTransactionForTest(t, tableScanTestName)
	tableScan := query.NewTableScan(transaction, tableName, layout)
	// ファイルのブロックを2つまで増やしておく.
	transaction.Append(fileName)

	t.Run("Delete 呼び出し時に、現在のレコードが削除される.", func(t *testing.T) {
		tableScan.Insert()
		tableScan.Delete()

		// 一度先頭に戻って再度Insertする.
		tableScan.BeforeFirst()
		tableScan.Insert()

		assert.Equal(t, record.NewRecordID(types.BlockNumber(0), record.SlotNumber(0)), tableScan.GetCurrentRecordID(), "current record id は recordID0 であるべし.")
	})
}

func TestTableScanNext(t *testing.T) {
	schema := buildTestTableSchema()
	layout := record.NewLayout(schema)

	tableName := types.TableName("test_table_scan_next")
	fileName := string(tableName) + ".table"

	transaction := newTransactionForTest(t, tableScanTestName)
	tableScan := query.NewTableScan(transaction, tableName, layout)
	// ファイルのブロックを2つまで増やしておく.
	transaction.Append(fileName)

	// 先頭ブロックの1つ目と3つ目のレコード, および2つ目のブロックの5つ目のレコードを InUse にしておく.
	tableScan.Insert()
	tableScan.Insert()
	tableScan.Insert()
	secondRecordID := record.NewRecordID(types.BlockNumber(0), record.SlotNumber(1))
	tableScan.MoveToRecordID(secondRecordID)
	tableScan.Delete()                                                                       // 先頭ブロックの2つ目のレコードを Empty にもどす.
	tableScan.MoveToRecordID(record.NewRecordID(types.BlockNumber(1), record.SlotNumber(3))) // Insert は後ろのレコードを見るので、4つ目のレコードにセットしておく.
	tableScan.Insert()
	assert.Equal(t, record.NewRecordID(types.BlockNumber(1), record.SlotNumber(4)), tableScan.GetCurrentRecordID(), "current record id は 2つ目のブロックの5つ目のレコードであるべし.")

	// 最初の位置に戻しておく.
	tableScan.BeforeFirst()

	t.Run("Next で次のレコードに移動できる.", func(t *testing.T) {
		hasNext := tableScan.Next()
		assert.True(t, hasNext, "次のレコードがあるので true が返る.")
		assert.Equal(t, record.NewRecordID(types.BlockNumber(0), record.SlotNumber(0)), tableScan.GetCurrentRecordID(), "current record id は 1つ目のレコードであるべし.")

		hasNext = tableScan.Next()
		assert.True(t, hasNext, "次のレコードがあるので true が返る.")
		assert.Equal(t, record.NewRecordID(types.BlockNumber(0), record.SlotNumber(2)), tableScan.GetCurrentRecordID(), "current record id は 3つ目のレコードであるべし.")

		hasNext = tableScan.Next()
		assert.True(t, hasNext, "次のレコードがあるので true が返る.")
		assert.Equal(t, record.NewRecordID(types.BlockNumber(1), record.SlotNumber(4)), tableScan.GetCurrentRecordID(), "current record id は 第２ブロックの5つ目のレコードであるべし.")
	})

	t.Run("Next で最後のレコードを超えると false が返る.", func(t *testing.T) {
		hasNext := tableScan.Next()
		assert.False(t, hasNext, "次のレコードがないので false が返る.")
	})
}
