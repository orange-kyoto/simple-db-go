package buffer

import (
	"simple-db-go/constants"
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/logger"
	"simple-db-go/types"
	"sync"
	"time"
)

const bufferPinWaitThreshold = constants.WAIT_THRESHOLD

var bufferManagerMutex sync.Mutex

type BufferManager struct {
	bufferPool   []*Buffer
	numAvailable types.Int
}

// NOTE: シングルトンにすることを検討したが、テストが複雑になりそうなのと、あくまで学習用のアプリケーションなので、特に複雑な管理はしない。
func NewBufferManager(fm *file.FileManager, lm *log.LogManager, bufferPoolSize types.Int) *BufferManager {
	bufferPool := make([]*Buffer, 0, bufferPoolSize)
	for i := types.Int(0); i < bufferPoolSize; i++ {
		buffer := NewBuffer(fm, lm)
		bufferPool = append(bufferPool, buffer)
	}

	bm := &BufferManager{
		bufferPool:   bufferPool,
		numAvailable: bufferPoolSize,
	}

	return bm
}

func (bm *BufferManager) Available() types.Int {
	logger.Infof("BufferManager.Available() called. numAvailable=%d", bm.numAvailable)
	bufferManagerMutex.Lock()
	defer bufferManagerMutex.Unlock()

	return bm.numAvailable
}

func (bm *BufferManager) FlushAll(transactionNumber types.TransactionNumber) {
	bufferManagerMutex.Lock()
	defer bufferManagerMutex.Unlock()

	for _, buffer := range bm.bufferPool {
		if buffer.ModifyingTransaction() == transactionNumber {
			buffer.flush()
		}
	}
}

func (bm *BufferManager) Unpin(buffer *Buffer) {
	logger.Infof("BufferManager.Unpin() called. buffer=%+v", buffer)
	bufferManagerMutex.Lock()
	defer bufferManagerMutex.Unlock()

	buffer.unpin()
	if !buffer.IsPinned() {
		bm.numAvailable++
	}
}

func (bm *BufferManager) Pin(blockID file.BlockID) *Buffer {
	logger.Infof("BufferManager.Pin() called. blockID=%+v", blockID)

	timeout := time.After(constants.WAIT_THRESHOLD)
	defer bufferManagerMutex.Unlock()

	for {
		bufferManagerMutex.Lock()

		buffer := bm.tryToPin(blockID)

		select {
		case <-timeout:
			panic("[BufferManager] ピンリクエストがタイムアウトしました.")
		default:
			if buffer != nil {
				return buffer
			} else {
				// デッドロックを防ぐために、次のループに進む前にロックを解放する.
				bufferManagerMutex.Unlock()
				break
			}
		}
	}
}

func (bm *BufferManager) tryToPin(blockID file.BlockID) *Buffer {
	buffer := bm.findExistngBuffer(blockID)
	if buffer == nil {
		buffer = bm.chooseUnpinnedBuffer()
		if buffer == nil {
			// note: 空いているバッファーがなかったパターン
			return nil
		} else {
			buffer.assignToBlock(blockID)
		}
	}

	if !buffer.IsPinned() {
		// note: ピンされていなかったバッファーを1つ潰したことになるので.
		bm.numAvailable--
	}

	buffer.pin()
	return buffer
}

func (bm *BufferManager) findExistngBuffer(blockID file.BlockID) *Buffer {
	for _, buffer := range bm.bufferPool {
		if buffer.GetBlockID() == blockID {
			return buffer
		}
	}
	return nil
}

func (bm *BufferManager) chooseUnpinnedBuffer() *Buffer {
	for _, buffer := range bm.bufferPool {
		if !buffer.IsPinned() {
			return buffer
		}
	}
	return nil
}
