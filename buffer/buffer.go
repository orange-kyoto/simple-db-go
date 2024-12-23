package buffer

import (
	"simple-db-go/file"
	"simple-db-go/log"
)

// TODO: いい感じのパッケージに移動したい.
type TransactionNum int

type Buffer struct {
	fileManager *file.FileManager
	logManager  *log.LogManager

	contents *file.Page
	blockID  *file.BlockID

	pinCount int

	transactionNum TransactionNum

	lsn log.LSN
}

func NewBuffer(fm *file.FileManager, lm *log.LogManager) *Buffer {
	return &Buffer{
		fileManager:    fm,
		logManager:     lm,
		contents:       file.NewPage(fm.BlockSize()),
		blockID:        nil,
		pinCount:       0,
		transactionNum: -1,
		lsn:            -1,
	}
}

func (b *Buffer) GetContents() *file.Page {
	return b.contents
}

func (b *Buffer) GetBlockID() *file.BlockID {
	return b.blockID
}

// もし lsn < 0 ならば、この呼び出し元での更新処理はログレコードを生成していないことを意味する.
func (b *Buffer) SetModified(transactionNum TransactionNum, lsn log.LSN) {
	b.transactionNum = transactionNum
	if lsn >= 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) IsPinned() bool {
	return b.pinCount > 0
}

func (b *Buffer) ModifyingTransaction() TransactionNum {
	return b.transactionNum
}

func (b *Buffer) assignToBlock(blockID *file.BlockID) {
	b.flush()
	b.blockID = blockID
	b.fileManager.Read(b.blockID, b.contents)
	b.pinCount = 0
}

func (b *Buffer) flush() {
	if b.transactionNum >= 0 {
		b.logManager.Flush(b.lsn)
		b.fileManager.Write(b.blockID, b.contents)
		b.transactionNum = -1
	}
}

func (b *Buffer) pin() {
	b.pinCount++
}

func (b *Buffer) unpin() {
	b.pinCount--
}
