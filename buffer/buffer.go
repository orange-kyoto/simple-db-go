package buffer

import (
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/types"
)

type Buffer struct {
	fileManager *file.FileManager
	logManager  *log.LogManager

	contents *file.Page
	blockID  file.BlockID

	pinCount types.Int

	transactionNumber types.TransactionNumber

	// この Buffer が保持する Page が変更された場合に、最新の LSN を保持する.
	// もし LSN が負の値ならば、その変更に該当するログレコードは作成されないことを意味する.
	lsn log.LSN
}

func NewBuffer(fm *file.FileManager, lm *log.LogManager) *Buffer {
	return &Buffer{
		fileManager: fm,
		logManager:  lm,
		contents:    file.NewPage(fm.BlockSize()),
		// blockID:           nil,
		pinCount:          0,
		transactionNumber: -1,
		lsn:               -1,
	}
}

func (b *Buffer) GetContents() *file.Page {
	return b.contents
}

func (b *Buffer) GetBlockID() file.BlockID {
	return b.blockID
}

// もし lsn < 0 ならば、この呼び出し元での更新処理はログレコードを生成していないことを意味する.
func (b *Buffer) SetModified(transactionNum types.TransactionNumber, lsn log.LSN) {
	b.transactionNumber = transactionNum
	if lsn >= 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) IsPinned() bool {
	return b.pinCount > 0
}

func (b *Buffer) ModifyingTransaction() types.TransactionNumber {
	return b.transactionNumber
}

func (b *Buffer) assignToBlock(blockID file.BlockID) {
	b.flush()
	b.blockID = blockID
	b.fileManager.Read(b.blockID, b.contents)
	b.pinCount = 0
}

func (b *Buffer) flush() {
	if b.transactionNumber >= 0 {
		// 先にログをディスクに書き込むのが大事
		b.logManager.Flush(b.lsn)
		b.fileManager.Write(b.blockID, b.contents)
		b.transactionNumber = -1
	}
}

func (b *Buffer) pin() {
	b.pinCount++
}

func (b *Buffer) unpin() {
	b.pinCount--
}
