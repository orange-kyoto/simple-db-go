package transaction

import (
	"fmt"
	"simple-db-go/file"
	"simple-db-go/types"
	"sync"
	"time"
)

// Exclusive Lock が割り当てられているときは`-1`.
// Shared Lock が割り当てられているときは、その割り当てられている数.
type LockValue types.Int

type LockTable struct {
	// TODO: ここはキーはポインタじゃない方がいいかもしれない. そもそも大きいデータ構造でないし.
	locks map[*file.BlockID]LockValue

	// ロックの解放待ちになっている goroutine に通知を送るための channel たち.
	waitList []chan bool

	requestChan chan lockTableRequest
	closeChan   chan bool
}

const LOCK_WAIT_THRESHOLD = 3 * time.Second

var (
	lockTableInstance *LockTable
	once              sync.Once
)

/*
Concurrency Manager は各トランザクションごとに用意するが、ロックを管理するためには Lock Table は1つだけ使うべきである。
なので、Singleton Pattern を使って Lock Table を実装する.
*/
func NewLockTable() *LockTable {
	once.Do(func() {
		lockTableInstance = &LockTable{
			locks:       make(map[*file.BlockID]LockValue),
			requestChan: make(chan lockTableRequest),
			closeChan:   make(chan bool),
		}
		go lockTableInstance.run()
	})

	return lockTableInstance
}

func (lt *LockTable) run() {
	for {
		select {
		case request := <-lt.requestChan:
			request.resolve(lt)
			break
		case <-lt.closeChan:
			close(lt.requestChan)
			close(lt.closeChan)
			return
		}
	}
}

func (lt *LockTable) SLock(blockID *file.BlockID) {
	requestFunc := func(blockID *file.BlockID, replyChan chan bool, waitChan chan bool) {
		req := &sLockRequest{
			blockID:   blockID,
			replyChan: replyChan,
			waitChan:  waitChan,
		}
		lt.requestChan <- req
	}

	doLockRequest(blockID, requestFunc)
}

func (lt *LockTable) XLock(blockID *file.BlockID) {
	requestFunc := func(blockID *file.BlockID, replyChan chan bool, waitChan chan bool) {
		req := &xLockRequest{
			blockID:   blockID,
			replyChan: replyChan,
			waitChan:  waitChan,
		}
		lt.requestChan <- req
	}

	doLockRequest(blockID, requestFunc)
}

func (lt *LockTable) Unlock(blockID *file.BlockID) {
	requstFunc := func(blockID *file.BlockID, replyChan chan bool, waitChan chan bool) {
		req := &unlockRequest{
			blockID:   blockID,
			replyChan: replyChan,
			waitChan:  waitChan,
		}
		lt.requestChan <- req
	}

	doLockRequest(blockID, requstFunc)
}

func doLockRequest(blockID *file.BlockID, requestFunc func(blockID *file.BlockID, replyChan chan bool, waitChan chan bool)) {
	replyChan := make(chan bool)
	waitChan := make(chan bool)
	defer close(replyChan)
	defer close(waitChan)

	timeout := time.After(LOCK_WAIT_THRESHOLD)

	for {
		requestFunc(blockID, replyChan, waitChan)

		select {
		// 処理が完了したならそれで処理を終了する
		case <-replyChan:
			return
		// 待ち状態が解除されたので、もう一度リクエストを送る
		case <-waitChan:
			continue
		case <-timeout:
			panic(fmt.Sprintf("Lock request timed out. blockID=%+v\n", blockID))
		}
	}
}

func (lt *LockTable) Close() {
	lt.closeChan <- true
}

type lockTableRequest interface {
	resolve(*LockTable)
}

type sLockRequest struct {
	blockID *file.BlockID
	// 処理が完了したことを知らせるためだけの channel
	replyChan chan bool
	// 待ち状態が解除されたことを通知するための channel
	waitChan chan bool
}

func (slr *sLockRequest) resolve(lockTable *LockTable) {
	// やること整理
	// XLock があるかチェック。なければ次に進むし、あれば waitList に追加してすぐ終了する.
	// lockValue を獲得する。
	// 新しい値を map に格納する。それで終了。ロックを獲得したことになっているのか。
	if lockTable.hasXLock(slr.blockID) {
		lockTable.waitList = append(lockTable.waitList, slr.waitChan)
		return
	}

	lockValue := lockTable.getLockValue(slr.blockID)
	lockTable.locks[slr.blockID] = lockValue + 1
	slr.replyChan <- true
}

type xLockRequest struct {
	blockID *file.BlockID
	// 処理が完了したことを知らせるためだけの channel
	replyChan chan bool
	// 待ち状態が解除されたことを通知するための channel
	waitChan chan bool
}

func (xlr *xLockRequest) resolve(lockTable *LockTable) {
	// 注意：ConcurrencyManager は XLock を獲得する前に必ず　SLock を獲得するという前提を置く.
	// つまり、XLock 獲得をリクエストしたトランザクションによって事前に SLock が獲得されているはずなので、LockValue は少なくとも1になっている.
	// また、その時点で、他のトランザクションが XLock を獲得していないことが保証される.
	// なので、ここでは他のトランザクションによって SLock が獲得されているかどうかだけチェックすれば十分.
	if lockTable.hasOtherSLocks(xlr.blockID) {
		lockTable.waitList = append(lockTable.waitList, xlr.waitChan)
		return
	}

	lockTable.locks[xlr.blockID] = -1
	xlr.replyChan <- true
}

type unlockRequest struct {
	blockID *file.BlockID
	// 処理が完了したことを知らせるためだけの channel
	replyChan chan bool
	// 待ち状態が解除されたことを通知するための channel
	waitChan chan bool
}

func (ulr *unlockRequest) resolve(lockTable *LockTable) {
	lockValue := lockTable.getLockValue(ulr.blockID)
	if lockValue > 1 {
		lockTable.locks[ulr.blockID] = lockValue - 1
	} else {
		delete(lockTable.locks, ulr.blockID)
		lockTable.notifyAll()
	}
	ulr.replyChan <- true
}

func (lt *LockTable) hasXLock(blockID *file.BlockID) bool {
	return lt.getLockValue(blockID) < 0
}

func (lt *LockTable) hasOtherSLocks(blockID *file.BlockID) bool {
	return lt.getLockValue(blockID) > 1
}

func (lt *LockTable) getLockValue(blockID *file.BlockID) LockValue {
	value, exists := lt.locks[blockID]
	if exists {
		return value
	} else {
		return 0
	}
}

func (lt *LockTable) notifyAll() {
	for _, waitChan := range lt.waitList {
		func(c chan bool) {
			// SLock/XLock リクエストがすでにタイムアウトしているなどで、waitChan がクローズされている可能性がある.
			// ここでは簡単に、recover でpanicを回避するだけにとどめる.
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("[LockTable.notifyAll] Panic recovered: channel is already closed. waitChan=%+v\n", c)
				}
			}()

			select {
			case c <- true:
				fmt.Printf("[LockTable.notifyAll] Succeeded sending a message to waitChan. waitChan=%+v\n", waitChan)
			default:
				fmt.Printf("[LockTable.notifyAll] channel is already closed. waitChan=%+v\n", waitChan)
			}
		}(waitChan)
	}
	lt.waitList = nil
}
