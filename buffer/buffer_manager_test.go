package buffer

import (
	"os"
	"path/filepath"
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/types"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	testDir     = "./test_buffer_manager"
	logFileName = "test.log"
	blockSize   = 16
)

func setup(t *testing.T) (*file.FileManager, *log.LogManager) {
	t.Helper()

	fm := file.NewFileManager(testDir, blockSize)
	lm := log.NewLogManager(fm, logFileName)

	return fm, lm
}

func cleanup(t *testing.T) {
	t.Helper()
	os.RemoveAll(testDir)
}

func TestBufferManagerInitialization(t *testing.T) {
	defer cleanup(t)

	fm := file.NewFileManager(testDir, blockSize)
	lm := log.NewLogManager(fm, logFileName)
	numBuffers := 3
	bm := NewBufferManager(fm, lm, types.Int(numBuffers))

	assert.Equal(t, types.Int(3), bm.numAvailable, "numAvailable should be 3")
}

func TestPinUnpinBuffer(t *testing.T) {
	defer cleanup(t)

	fm, lm := setup(t)
	numBuffers := 2
	bm := NewBufferManager(fm, lm, types.Int(numBuffers))

	// テスト用のファイルを用意しておく
	os.WriteFile(filepath.Join(testDir, "test_file"),
		[]byte("Test File For Buffer Manager Test. This is a dummy file."),
		0644)
	firstBlockID := file.NewBlockID("test_file", 0)
	secondBlockID := file.NewBlockID("test_file", 1)
	thirdBlockID := file.NewBlockID("test_file", 2)

	// 一旦テストケースを整理
	// 観点を整理する
	// 観点1. すでにバッファリングされている BlockID かどうか.
	// 観点2. 対象のバッファーがピンされているかどうか.
	// 観点3. ピンされているバッファーが残っているかどうか.
	//
	// 注意：指定されたブロックIDのファイルは存在しているべきなのか？→Yes!なぜなら、ピンするということは、バッファーにファイルの内容を読み取るわけなので、Page をメモリに読み込むことになる！
	// というわけで、テスト用のファイルを用意しておいた方が良い。FileManager のメソッドをそのまま使えるだろう。

	t.Run("[条件]まだバッファーされていない [期待値]バッファーされている", func(t *testing.T) {
		testBuffer := bm.Pin(firstBlockID)
		assert.True(t, bm.bufferPool[0].IsPinned(), "Buffer[0] should be pinned")
		assert.True(t, !bm.bufferPool[1].IsPinned(), "Buffer[1] should NOT be pinned")
		assert.Equal(t, types.Int(1), testBuffer.pinCount, "Pin count should be 1")
		assert.Equal(t, types.Int(1), bm.numAvailable, "numAvailable should be 1")
	})

	t.Run("[条件]すでにバッファーされている＆ピンされている [期待値]ピンカウントが増えている", func(t *testing.T) {
		// すでにピンされているブロックをさらにピンする
		buffer := bm.Pin(firstBlockID)

		assert.True(t, bm.bufferPool[0].IsPinned(), "Buffer[0] should be pinned")
		assert.True(t, !bm.bufferPool[1].IsPinned(), "Buffer[1] should NOT be pinned")
		assert.Equal(t, types.Int(2), buffer.pinCount, "Pin count should be 2")
		assert.Equal(t, types.Int(1), bm.numAvailable, "numAvailable should be 1")
	})

	t.Run("[条件]すでにバッファーされている＆ピンされていない [期待値]ピンされている", func(t *testing.T) {
		// 2つピンされているので、2回unpinする
		pinnedBuffer := bm.bufferPool[0]
		bm.Unpin(pinnedBuffer)
		bm.Unpin(pinnedBuffer)
		assert.True(t, !bm.bufferPool[0].IsPinned(), "Buffer[0] should NOT be pinned")
		assert.True(t, !bm.bufferPool[1].IsPinned(), "Buffer[1] should NOT be pinned")
		assert.Equal(t, types.Int(2), bm.numAvailable, "numAvailable should be 2")

		// さらにピンする
		bm.Pin(firstBlockID)
		assert.True(t, bm.bufferPool[0].IsPinned(), "Buffer[0] should be pinned")
		assert.True(t, !bm.bufferPool[1].IsPinned(), "Buffer[1] should NOT be pinned")
		assert.Equal(t, types.Int(1), bm.numAvailable, "numAvailable should be 1")
	})

	t.Run("[条件]空いているバッファーがない場合にPinしようとし、タイムアウトを過ぎた [期待値]パニックする", func(t *testing.T) {
		// 空いている2つ目のバッファーをピンしておき、バッファープールが埋まるようにする.
		bm.Pin(secondBlockID)
		assert.True(t, bm.bufferPool[0].IsPinned(), "Buffer[0] should be pinned")
		assert.True(t, bm.bufferPool[1].IsPinned(), "Buffer[1] should be pinned")
		assert.Equal(t, types.Int(0), bm.numAvailable, "numAvailable should be 0")

		// 追加でピンしようとする. 他の goroutine がアンピンしないので、タイムアウトする.
		assert.Panics(t, func() {
			bm.Pin(thirdBlockID)
		})
	})

	t.Run("[条件]空いているバッファーがない場合にPinしようとしたが、タイムアウト前にプールの空きができた [期待値]Pinできる", func(t *testing.T) {
		assert.True(t, bm.bufferPool[0].IsPinned(), "Buffer[0] should be pinned")
		assert.True(t, bm.bufferPool[1].IsPinned(), "Buffer[1] should be pinned")
		assert.Equal(t, types.Int(0), bm.numAvailable, "numAvailable should be 0")
		assert.Len(t, bm.waitList, 1, "len(bm.waitList) should be 1 (前のテストで1つ残っているため).")

		// 追加でピンしようとする.
		done := make(chan bool)
		go func() {
			defer close(done)
			pinnedBuffer := bm.Pin(thirdBlockID)
			if !assert.NotNil(t, pinnedBuffer, "Pin request should be successful.") {
				t.Errorf("Pin request should be successful. thirdBlockID=%+v\n", thirdBlockID)
			}
		}()

		// 別の goroutine でアンピンする
		done2 := make(chan bool)
		go func() {
			defer close(done2)
			time.Sleep(500 * time.Millisecond)
			bm.Unpin(bm.bufferPool[0])
		}()

		// 両方の goroutine が終了するまで待つ
		<-done
		<-done2

		assert.True(t, bm.bufferPool[0].IsPinned(), "Buffer[0] should be pinned")
		assert.True(t, bm.bufferPool[1].IsPinned(), "Buffer[1] should be pinned")
		assert.Equal(t, types.Int(0), bm.numAvailable, "numAvailable should be 0")
	})
}
