package record

import (
	"os"
	"simple-db-go/buffer"
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/transaction"
	"simple-db-go/types"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	fileManager   *file.FileManager
	logManager    *log.LogManager
	bufferManager *buffer.BufferManager
	mu            sync.Mutex
)

func startManagers() {
	mu.Lock()
	defer mu.Unlock()

	if fileManager == nil {
		fileManager = file.NewFileManager("test_record_page", 512)
	}
	if logManager == nil {
		logManager = log.NewLogManager(fileManager, "test_record_page.log")
	}
	if bufferManager == nil {
		bufferManager = buffer.NewBufferManager(fileManager, logManager, 10)
	}
}

func cleanup() {
	os.RemoveAll("test_record_page")
}

func TestMain(m *testing.M) {
	cleanup()
	code := m.Run()
	cleanup()
	os.Exit(code)
}

func startNewTransaction() *transaction.Transaction {
	startManagers()
	return transaction.NewTransaction(fileManager, logManager, bufferManager)
}

func buildTestSchema() *Schema {
	schema := NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 10)
	schema.AddIntField("age")
	return schema
}

func TestRecordPageFormat(t *testing.T) {
	transaction := startNewTransaction()

	fileName := "test_record_page_format.table"
	blockID := transaction.Append(fileName)
	transaction.Pin(blockID)
	defer transaction.Unpin(blockID)

	schema := buildTestSchema()
	layout := NewLayout(schema)

	recordPage := NewRecordPage(transaction, *blockID, layout)
	recordPage.Format()

	t.Run("全てのスロットが初期値で初期化されている.", func(t *testing.T) {
		blockSize := types.Int(512)
		slotSize := types.Int(4 + 4 + (4 + 10) + 4) // 30

		slotNumber := SlotNumber(0)
		for ; types.Int(slotNumber+1)*slotSize < blockSize; slotNumber++ {
			slotOffset := types.Int(slotNumber) * slotSize
			idFieldOffset := slotOffset + 4
			nameFieldOffset := idFieldOffset + 4
			ageFieldOffset := nameFieldOffset + (4 + 10)

			// トランザクション経由で直接データを検証する.
			assert.Equalf(t, types.Int(SLOT_EMPTY), transaction.GetInt(blockID, slotOffset), "slot %d's flag is empty", slotNumber)
			assert.Equalf(t, types.Int(0), transaction.GetInt(blockID, idFieldOffset), "slot %d's id is 0", slotNumber)
			assert.Equalf(t, "", transaction.GetString(blockID, nameFieldOffset), "slot %d's name is empty", slotNumber)
			assert.Equalf(t, types.Int(0), transaction.GetInt(blockID, ageFieldOffset), "slot %d's age is 0", slotNumber)

			// レコードページで実装するメソッドでも同じ結果になることを確認する.
			assert.Equalf(t, types.Int(0), recordPage.GetInt(slotNumber, "id"), "slot %d's id is 0", slotNumber)
			assert.Equalf(t, "", recordPage.GetString(slotNumber, "name"), "slot %d's name is empty", slotNumber)
			assert.Equalf(t, types.Int(0), recordPage.GetInt(slotNumber, "age"), "slot %d's age is 0", slotNumber)
		}

		// 512 / 30 = 17.06666666666 = 17個スロットがあるはず.
		// slot_number は 0 から始まる.
		expectedSlotNumber := SlotNumber(18)
		assert.Equal(t, expectedSlotNumber, slotNumber-1, "slot number should be 18.") // ループ終了時には1つ余分にインクリメントされているため、-1 する.
	})
}

func TestRecordPageFindUsedSlotAfter(t *testing.T) {
	transaction := startNewTransaction()

	fileName := "test_record_page_find_used_slot_after.table"
	blockID := transaction.Append(fileName)
	transaction.Pin(blockID)
	defer transaction.Unpin(blockID)

	schema := buildTestSchema()
	layout := NewLayout(schema)

	recordPage := NewRecordPage(transaction, *blockID, layout)
	recordPage.Format()

	t.Run("全て空きスロットの場合、常にスロット番号が NULL を返す.", func(t *testing.T) {
		for slotNumber := SlotNumber(-1); slotNumber <= 18; slotNumber++ {
			result := recordPage.FindUsedSlotAfter(slotNumber)
			assert.Equalf(t, NULL_SLOT_NUMBER, result, "どのスロットも空いているため、スロット番号は NULL_SLOT_NUMBER である.(slot_number=%d)\n", slotNumber)
		}
	})

	// いくつかのスロットを使用する.
	recordPage.setSlotFlag(SlotNumber(0), SLOT_INUSE)
	recordPage.setSlotFlag(SlotNumber(2), SLOT_INUSE)

	t.Run("使用されているスロット番号を正しく返すこと.", func(t *testing.T) {
		result1 := recordPage.FindUsedSlotAfter(SlotNumber(-1))
		result2 := recordPage.FindUsedSlotAfter(SlotNumber(0))
		result3 := recordPage.FindUsedSlotAfter(SlotNumber(1))
		result4 := recordPage.FindUsedSlotAfter(SlotNumber(2))

		assert.Equal(t, SlotNumber(0), result1, "スロット番号-1から探索した場合、次の使用されているスロット番号は 0 である.")
		assert.Equal(t, SlotNumber(2), result2, "スロット番号0から探索した場合、次の使用されているスロット番号は 2 である.")
		assert.Equal(t, SlotNumber(2), result3, "スロット番号1から探索した場合、次の使用されているスロット番号は 2 である.")
		assert.Equal(t, NULL_SLOT_NUMBER, result4, "スロット番号2から探索した場合、次の使用されているスロット番号は存在しない.")
	})
}

func TestRecordPageFindEmptySlotAfter(t *testing.T) {
	transaction := startNewTransaction()

	fileName := "test_record_page_find_empty_slot_after.table"
	blockID := transaction.Append(fileName)
	transaction.Pin(blockID)
	defer transaction.Unpin(blockID)

	schema := buildTestSchema()
	layout := NewLayout(schema)

	recordPage := NewRecordPage(transaction, *blockID, layout)
	recordPage.Format()

	t.Run("全て空きスロットの場合、常に次のスロット番号が返る. ただし、最後のスロットから探索した場合はNULLになる.", func(t *testing.T) {
		for slotNumber := SlotNumber(-1); slotNumber <= 18; slotNumber++ {
			result := recordPage.FindEmptySlotAfter(slotNumber)

			if slotNumber < 18 {
				assert.Equalf(t, slotNumber+1, result, "どのスロットも空いているため、次のスロット番号は %d である.(slot_number=%d)\n", slotNumber+1, slotNumber)
			} else {
				assert.Equalf(t, NULL_SLOT_NUMBER, result, "最後のスロットから探索した場合、次のスロット番号は NULL_SLOT_NUMBER である.(slot_number=%d)\n", slotNumber)
			}
		}
	})

	// もう一度初期化する
	recordPage.Format()

	t.Run("空いているスロット番号を見つけた後に再度探索した際、既に使われているので次のスロット番号が返されること.", func(t *testing.T) {
		result1 := recordPage.FindEmptySlotAfter(SlotNumber(-1))
		result2 := recordPage.FindEmptySlotAfter(SlotNumber(-1))

		assert.Equal(t, SlotNumber(0), result1, "スロット番号-1から探索した場合、次の空いているスロット番号は 0 である.")
		assert.Equal(t, SlotNumber(1), result2, "スロット番号-1から探索した場合、次の空いているスロット番号は 1 である(0は先に使用された).")
	})
}

func TestRecordPageDelete(t *testing.T) {
	transaction := startNewTransaction()

	fileName := "test_record_page_delete.table"
	blockID := transaction.Append(fileName)
	transaction.Pin(blockID)
	defer transaction.Unpin(blockID)

	schema := buildTestSchema()
	layout := NewLayout(schema)

	recordPage := NewRecordPage(transaction, *blockID, layout)
	recordPage.Format()

	t.Run("Delete実行後のスロットが FindUsedSlotAfter で返されないこと.", func(t *testing.T) {
		slotNumber := recordPage.FindEmptySlotAfter(SlotNumber(3))
		assert.Equal(t, SlotNumber(4), slotNumber, "スロット番号4は空いている.")

		usedSlotNumber := recordPage.FindUsedSlotAfter(SlotNumber(-1))
		assert.Equal(t, SlotNumber(4), usedSlotNumber, "スロット番号4は使用されている.")

		recordPage.Delete(SlotNumber(4))

		usedSlotNumber = recordPage.FindUsedSlotAfter(SlotNumber(-1))
		assert.Equal(t, NULL_SLOT_NUMBER, usedSlotNumber, "スロット番号4は使用されていない.")
	})
}
