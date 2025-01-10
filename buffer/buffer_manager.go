package buffer

import (
	"fmt"
	"simple-db-go/constants"
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/logger"
	"simple-db-go/types"
	"time"
)

const bufferPinWaitThreshold = constants.WAIT_THRESHOLD

type BufferManager struct {
	bufferPool   []*Buffer
	numAvailable types.Int

	waitList []chan bool

	requestChan chan BufferRequest
	closeChan   chan bool
}

// NOTE: シングルトンにすることを検討したが、テストが複雑になりそうなのと、あくまで学習用のアプリケーションなので、特に複雑な管理はしない。
func NewBufferManager(fm *file.FileManager, lm *log.LogManager, bufferPoolSize types.Int) *BufferManager {
	bufferPool := make([]*Buffer, 0, bufferPoolSize)
	for i := 0; i < int(bufferPoolSize); i++ {
		bufferPool = append(bufferPool, NewBuffer(fm, lm))
	}

	requestChan := make(chan BufferRequest)
	closeChan := make(chan bool)

	bm := &BufferManager{
		bufferPool:   bufferPool,
		numAvailable: bufferPoolSize,
		requestChan:  requestChan,
		closeChan:    closeChan,
	}

	go bm.run()

	return bm
}

func (bm *BufferManager) run() {
	defer close(bm.requestChan)
	defer close(bm.closeChan)

	for {
		select {
		case req := <-bm.requestChan:
			logger.Debugf("BufferManager received request. req=%+v, type=%T", req, req)
			req.resolve(bm)
		case <-bm.closeChan:
			// ここでするべきことってなんだろう？
			// バッファの内容を全てファイルに書き込む？
			// TODO: もしかしたら recovery manager とかの仕事かも？一旦何もしないことにする。
			return
		}
	}
}

func (bm *BufferManager) Available() types.Int {
	replyChan := make(chan types.Int)
	defer close(replyChan)

	req := &AvailableBuffersRequest{
		replyChan: replyChan,
	}

	bm.requestChan <- req
	return <-replyChan
}

func (bm *BufferManager) FlushAll(transactionNumber types.TransactionNumber) {
	replyChan := make(chan bool)
	defer close(replyChan)

	req := &FlushAllRequest{
		transactionNumber: transactionNumber,
		replyChan:         replyChan,
	}

	bm.requestChan <- req
	<-replyChan
}

func (bm *BufferManager) Unpin(buffer *Buffer) {
	replyChan := make(chan bool)
	defer close(replyChan)
	req := &UnpinRequest{
		buffer:    buffer,
		replyChan: replyChan,
	}

	bm.requestChan <- req
	<-replyChan
}

func (bm *BufferManager) Pin(blockID file.BlockID) *Buffer {
	logger.Debugf("Call Pin. bm.numAvailable=%d, blockID=%+v", bm.numAvailable, blockID)
	replyChan := make(chan *Buffer)
	waitChan := make(chan bool)
	defer close(replyChan)
	defer close(waitChan)

	// waitChan からの受信を待つ.
	// もし受信できてかつ、replyChan に値が入っていればそれを返す。終了。
	// waitChan から受信できたが、replyChan に値が入っていない場合はリトライをする。
	// これを繰り返す。
	// もしreplyChanから受信できないままタイムアウトを過ぎたらpanicさせる.

	doRequest := func() {
		req := &PinRequest{
			blockID:   blockID,
			replyChan: replyChan,
			waitChan:  waitChan,
		}
		bm.requestChan <- req
	}

	timeout := time.After(bufferPinWaitThreshold)

	for {
		doRequest()

		select {
		case buffer := <-replyChan:
			if buffer != nil {
				return buffer
			}
			continue
		case <-waitChan:
			// 何もしない. もう一度リクエストを送るところからやり直す.
			continue
		case <-timeout:
			panic(fmt.Sprintf("pin request timed out. blockID=%+v, buffer_manager=%+v", blockID, bm))
		}
	}
}

func (bm *BufferManager) Close() {
	bm.closeChan <- true
}

type BufferRequest interface {
	resolve(bm *BufferManager)
}

type AvailableBuffersRequest struct {
	replyChan chan types.Int
}

func (abr *AvailableBuffersRequest) resolve(bm *BufferManager) {
	abr.replyChan <- bm.numAvailable
}

type FlushAllRequest struct {
	transactionNumber types.TransactionNumber
	// 完了したことの通知だけするためのチャンネル. 値は使わない.
	replyChan chan bool
}

func (far *FlushAllRequest) resolve(bm *BufferManager) {
	for _, buffer := range bm.bufferPool {
		if buffer.ModifyingTransaction() == far.transactionNumber {
			buffer.flush()
		}
	}
	far.replyChan <- true
}

type UnpinRequest struct {
	buffer *Buffer
	// 完了したことの通知だけするためのチャンネル. 値は使わない.
	replyChan chan bool
}

func (ur *UnpinRequest) resolve(bm *BufferManager) {
	ur.buffer.unpin()
	if !ur.buffer.IsPinned() {
		bm.numAvailable++
		bm.notifyAll()
	}
	ur.replyChan <- true
}

// 注意：UnpinRequest.resolve の中でだけ呼ばれるので、これについての排他制御はしない。
func (bm *BufferManager) notifyAll() {
	for _, waitChan := range bm.waitList {
		func(c chan bool) {
			// Pinリクエストがすでにタイムアウトしているなどで、waitChan がクローズされている可能性がある.
			// ここでは簡単に、recover でpanicを回避するだけにとどめる.
			defer func() {
				if r := recover(); r != nil {
					logger.Debugf("Panic recovered: channel is already closed. waitChan=%+v", c)
				}
			}()

			select {
			case c <- true:
				logger.Debugf("Succeeded sending a message to waitChan. waitChan=%+v", waitChan)
			default:
				logger.Debugf("Failed sending a message to waitChan. waitChan=%+v", waitChan)
			}
		}(waitChan)
	}
	bm.waitList = nil
}

type PinRequest struct {
	blockID   file.BlockID
	replyChan chan *Buffer
	// 空いているバッファーがない場合に待つための channel
	waitChan chan bool
}

func (pr *PinRequest) resolve(bm *BufferManager) {
	buffer := bm.tryToPin(pr.blockID)

	// pin できなかった場合
	// ---> wait list に追加して終了すれば良い. リトライやタイムアウト処理は呼び出し側で制御する.
	if buffer == nil {
		bm.waitList = append(bm.waitList, pr.waitChan)
		return
	}

	// pin できた場合
	// ---> これは素直に返せば良い
	pr.replyChan <- buffer
	return
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
	// TODO: これO(1)に改善できそう
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
