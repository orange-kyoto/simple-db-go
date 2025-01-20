package transaction

import (
	"simple-db-go/file"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConcurrencyManagerLock(t *testing.T) {
	t.Run("SLock が成功する.", func(t *testing.T) {
		t.Parallel()
		cm := NewConcurrencyManager()
		blockID := file.NewBlockID("test_concurrency_manager_1", 0)
		cm.SLock(blockID)

		lockType, exists := cm.locks[blockID]
		assert.Equal(t, sLOCK, lockType, "SLock は成功するので locks に要素が存在するべき.")
		assert.True(t, exists, "SLock は成功するので locks に要素が存在するべき.")
	})

	t.Run("XLock が成功する.", func(t *testing.T) {
		t.Parallel()
		cm := NewConcurrencyManager()
		blockID := file.NewBlockID("test_concurrency_manager_2", 0)
		cm.XLock(blockID)

		_, exists := cm.locks[blockID]
		if !exists {
			t.Errorf("XLock は成功するので locks に要素が存在するべき.")
		}
	})

	t.Run("Release が成功する.", func(t *testing.T) {
		t.Parallel()
		cm := NewConcurrencyManager()
		blockID := file.NewBlockID("test_concurrency_manager_3", 0)

		cm.SLock(blockID)
		cm.XLock(blockID)
		cm.Release()

		assert.Len(t, cm.locks, 0, "Release は成功するので LockTable の locks は空になるべき.")
		assert.NotPanics(t, func() { cm.SLock(blockID) }, "Release は成功するので再度ロックを獲得できる.")
	})

	t.Run("他のConcurrencyManagerでSLock が獲得されていても SLock を獲得できる.", func(t *testing.T) {
		t.Parallel()
		cm1 := NewConcurrencyManager()
		cm2 := NewConcurrencyManager()
		blockID := file.NewBlockID("test_concurrency_manager_4", 0)

		cm1.SLock(blockID)
		assert.NotPanics(t, func() { cm2.SLock(blockID) }, "他のConcurrencyManager で SLock が獲得されていても SLock を獲得できる.")
	})

	t.Run("他のConcurrencyManagerでSLock が獲得されていた場合、XLock を獲得できない.", func(t *testing.T) {
		t.Parallel()
		cm1 := NewConcurrencyManager()
		cm2 := NewConcurrencyManager()
		blockID := file.NewBlockID("test_concurrency_manager_5", 0)

		cm1.SLock(blockID)
		assert.Panics(t, func() { cm2.XLock(blockID) }, "他のConcurrencyManager で SLock が獲得されている場合、XLock を獲得できない.")
	})

	t.Run("他のConcurrencyManagerでXLockが獲得されていた場合、XLockを獲得できない.", func(t *testing.T) {
		t.Parallel()
		cm1 := NewConcurrencyManager()
		cm2 := NewConcurrencyManager()
		blockID := file.NewBlockID("test_concurrency_manager_6", 0)

		cm1.XLock(blockID)
		assert.Panics(t, func() { cm2.XLock(blockID) }, "他のConcurrencyManager で SLock が獲得されている場合、XLock を獲得できない.")
	})

	t.Run("他のConcurrencyManagerでSLockが獲得されていた際にXLockがブロックされ、その間にロックが解放されると、ロックが獲得できる.", func(t *testing.T) {
		t.Parallel()
		cm1 := NewConcurrencyManager()
		cm2 := NewConcurrencyManager()
		blockID := file.NewBlockID("test_concurrency_manager_7", 0)

		cm1.SLock(blockID)

		done2 := make(chan bool)
		go func() {
			defer close(done2)
			cm2.XLock(blockID)
		}()

		done1 := make(chan bool)
		go func() {
			defer close(done1)
			time.Sleep(500 * time.Millisecond)
			cm1.Release()
		}()

		<-done1
		<-done2

		lockType, exists := cm2.locks[blockID]
		assert.Equal(t, xLOCK, lockType, "他のConcurrencyManager で SLock が獲得されていた際に XLock が解放されると XLock を獲得できる.")
		assert.True(t, exists, "他のConcurrencyManager で SLock が獲得されていた際に XLock が解放されると XLock を獲得できる.")
	})
}
