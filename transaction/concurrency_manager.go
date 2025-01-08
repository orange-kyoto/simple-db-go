package transaction

import (
	"simple-db-go/file"
)

type lockType string

const (
	xLOCK lockType = "X"
	sLOCK lockType = "S"
)

/*
- 各トランザクションがそれ自身の ConcurrencyManager を持っている。
*/
type ConcurrencyManager struct {
	lockTable *LockTable
	// 当該トランザクションが保持しているロックを表現する.
	// ポインタでなく値で保持する.
	locks map[file.BlockID]lockType
}

func NewConcurrencyManager() *ConcurrencyManager {
	lockTable := NewLockTable()
	locks := make(map[file.BlockID]lockType)

	return &ConcurrencyManager{
		lockTable: lockTable,
		locks:     locks,
	}
}

// ロックを獲得していない場合のみ LockTable 経由でロックを獲得する.
func (cm *ConcurrencyManager) SLock(blockID file.BlockID) {
	_, exists := cm.locks[blockID]
	if !exists {
		cm.lockTable.SLock(blockID)
		cm.locks[blockID] = sLOCK
	}
}

// ロックを獲得していない場合のみ LockTable 経由でロックを獲得する.
func (cm *ConcurrencyManager) XLock(blockID file.BlockID) {
	// 注意：LockTable の実装で、XLock の前に SLock を獲得することが前提になっている.
	if !cm.hasXLock(blockID) {
		cm.SLock(blockID)
		cm.lockTable.XLock(blockID)
		cm.locks[blockID] = xLOCK
	}
}

// トランザクションが終了するときに全てのロックを解放する.
func (cm *ConcurrencyManager) Release() {
	for blockID := range cm.locks {
		cm.lockTable.Unlock(blockID)
	}
	cm.locks = make(map[file.BlockID]lockType)
}

func (cm *ConcurrencyManager) hasXLock(blockID file.BlockID) bool {
	lockType, exists := cm.locks[blockID]
	return exists && lockType == xLOCK
}
