package buffer

import (
	"fmt"
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/types"
	"time"

	"github.com/google/go-cmp/cmp"
)

const (
	WAIT_THRESHOLD = 3 * time.Second
)

type BufferManager struct {
	bufferPool   []*Buffer
	numAvailable int

	waitList []chan bool

	requestChan chan BufferRequest
	closeChan   chan bool
}

func NewBufferManager(fm *file.FileManager, lm *log.LogManager, numBuffers int) *BufferManager {
	bufferPool := make([]*Buffer, 0, numBuffers)
	for i := 0; i < numBuffers; i++ {
		bufferPool = append(bufferPool, NewBuffer(fm, lm))
	}

	requestChan := make(chan BufferRequest)
	closeChan := make(chan bool)

	bm := &BufferManager{
		bufferPool:   bufferPool,
		numAvailable: numBuffers,
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
			fmt.Printf("BufferManager received request. req=%+v, type=%T\n", req, req)
			req.resolve(bm)
		case <-bm.closeChan:
			// ここでするべきことってなんだろう？
			// バッファの内容を全てファイルに書き込む？
			// TODO: もしかしたら recovery manager とかの仕事かも？一旦何もしないことにする。
			return
		}
	}
}

func (bm *BufferManager) Available() int {
	replyChan := make(chan int)
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

func (bm *BufferManager) Pin(blockID *file.BlockID) *Buffer {
	fmt.Printf("Call Pin. bm.numAvailable=%d, blockID=%+v\n", bm.numAvailable, blockID)
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

	timeout := time.After(WAIT_THRESHOLD)

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
			panic(fmt.Sprintf("pin request timed out. blockID=%+v", blockID))
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
	replyChan chan int
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
	fmt.Printf("Call notifyAll. waitList=%+v\n", bm.waitList)
	for _, waitChan := range bm.waitList {
		func(c chan bool) {
			// Pinリクエストがすでにタイムアウトしているなどで、waitChan がクローズされている可能性がある.
			// ここでは簡単に、recover でpanicを回避するだけにとどめる.
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("[notifyAll] Panic recovered: channel is already closed. waitChan=%+v\n", c)
				}
			}()

			select {
			case c <- true:
				fmt.Printf("[notifyAll] Succeeded sending a message to waitChan. waitChan=%+v\n", waitChan)
			default:
				fmt.Printf("[notifyAll] channel is already closed. waitChan=%+v\n", waitChan)
			}
		}(waitChan)
	}
	bm.waitList = nil
	fmt.Printf("Finish notifyAll. waitList=%+v\n", bm.waitList)
}

type PinRequest struct {
	blockID   *file.BlockID
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

func (bm *BufferManager) tryToPin(blockID *file.BlockID) *Buffer {
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

func (bm *BufferManager) findExistngBuffer(blockID *file.BlockID) *Buffer {
	// TODO: これO(1)に改善できそう
	for _, buffer := range bm.bufferPool {
		if cmp.Equal(buffer.GetBlockID(), blockID) {
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
