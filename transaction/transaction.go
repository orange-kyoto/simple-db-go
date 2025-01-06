package transaction

import (
	"fmt"
	"simple-db-go/buffer"
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/types"
)

const END_OF_FILE types.Int = -1

type Transaction struct {
	recoveryManager    *RecoveryManager
	concurrencyManager *ConcurrencyManager
	bufferManager      *buffer.BufferManager
	fileManager        *file.FileManager
	transactionNumber  types.TransactionNumber
	bufferList         *BufferList
}

func NewTransaction(
	fm *file.FileManager,
	lm *log.LogManager,
	bm *buffer.BufferManager,
) *Transaction {
	transactionNumber := NextTransactionNumber()
	concurrencyManager := NewConcurrencyManager()
	bufferList := NewBufferList(bm)

	t := &Transaction{
		concurrencyManager: concurrencyManager,
		bufferManager:      bm,
		fileManager:        fm,
		transactionNumber:  transactionNumber,
		bufferList:         bufferList,
	}

	// NOTE: RecoveryManager の開始時に START レコードを書き込んでいる.
	recoveryManager := NewRecoveryManager(t, transactionNumber, lm, bm)
	t.recoveryManager = recoveryManager

	return t
}

func (t *Transaction) Commit() {
	t.recoveryManager.Commit()
	t.concurrencyManager.Release()
	t.bufferList.UnpinAll()
	fmt.Printf("transaction %d committed.\n", t.transactionNumber)
}

func (t *Transaction) Rollback() {
	t.recoveryManager.Rollback()
	t.concurrencyManager.Release()
	t.bufferList.UnpinAll()
	fmt.Printf("transaction %d rolled back.\n", t.transactionNumber)
}

// NOTE: 他のトランザクションも含めて、完了していないトランザクションの変更を全て Undo する.
func (t *Transaction) Recover() {
	t.bufferManager.FlushAll(t.transactionNumber)
	t.recoveryManager.Recover()
}

// 注意：呼び出し元には buffer の存在を知らせない.(それなら、他のメソッドでよしなに呼べばいいのでは？)
func (t *Transaction) Pin(blockID *file.BlockID) {
	t.bufferList.Pin(blockID)
}

func (t *Transaction) Unpin(blockID *file.BlockID) {
	t.bufferList.Unpin(blockID)
}

func (t *Transaction) GetInt(blockID *file.BlockID, offset types.Int) types.Int {
	t.concurrencyManager.SLock(blockID)
	buffer := t.bufferList.GetBuffer(blockID)
	return buffer.GetContents().GetInt(offset)
}

func (t *Transaction) GetString(blockID *file.BlockID, offset types.Int) string {
	t.concurrencyManager.SLock(blockID)
	buffer := t.bufferList.GetBuffer(blockID)
	return buffer.GetContents().GetString(offset)
}

func (t *Transaction) SetInt(blockID *file.BlockID, offset types.Int, val types.Int, okToLog bool) {
	t.concurrencyManager.XLock(blockID)
	buffer := t.bufferList.GetBuffer(blockID)
	var lsn log.LSN = -1
	if okToLog {
		// 注意：ここでログレコードに記録している.
		lsn = t.recoveryManager.SetInt(buffer, offset, val)
	}
	page := buffer.GetContents()
	page.SetInt(offset, val)
	buffer.SetModified(t.transactionNumber, lsn)
}

func (t *Transaction) SetString(blockID *file.BlockID, offset types.Int, val string, okToLog bool) {
	t.concurrencyManager.XLock(blockID)
	buffer := t.bufferList.GetBuffer(blockID)
	var lsn log.LSN = -1
	if okToLog {
		// 注意：ここでログレコードに記録している.
		lsn = t.recoveryManager.SetString(buffer, offset, val)
	}
	page := buffer.GetContents()
	page.SetString(offset, val)
	buffer.SetModified(t.transactionNumber, lsn)
}

// 注意：呼び出し側から buffer の存在を隠蔽している.
func (t *Transaction) AvailableBuffers() types.Int {
	return t.bufferManager.Available()
}

func (t *Transaction) BlockSize() types.Int {
	return t.fileManager.BlockSize()
}

// 注意：End Of File marker に対してのロックを獲得して排他制御をする.
func (t *Transaction) Size(filename string) types.Int {
	dummyBlockID := file.NewBlockID(filename, END_OF_FILE)
	t.concurrencyManager.SLock(dummyBlockID)
	return t.fileManager.GetBlockLength(filename)
}

// 注意：End Of File marker に対してのロックを獲得して排他制御をする.
func (t *Transaction) Append(filename string) *file.BlockID {
	dummyBlockID := file.NewBlockID(filename, END_OF_FILE)
	t.concurrencyManager.XLock(dummyBlockID)
	return t.fileManager.Append(filename)
}
