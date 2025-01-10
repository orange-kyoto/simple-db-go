package buffer

import (
	"os"
	"path/filepath"
	"simple-db-go/file"
	"simple-db-go/types"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPinUnpinBuffer(t *testing.T) {
	bufferManager := getBufferManagerForTest(t)

	// テスト用のファイルを用意しておく
	os.WriteFile(filepath.Join(bufferManagerTestName, "test_file"),
		[]byte("Test File For Buffer Manager Test. This is a dummy file."),
		0644)
	firstBlockID := file.NewBlockID("test_file", 0)
	secondBlockID := file.NewBlockID("test_file", 1)
	thirdBlockID := file.NewBlockID("test_file", 2)
	forthBlockID := file.NewBlockID("test_file", 3)

	// 一旦テストケースを整理
	// 観点を整理する
	// 観点1. すでにバッファリングされている BlockID かどうか.
	// 観点2. 対象のバッファーがピンされているかどうか.
	// 観点3. ピンされているバッファーが残っているかどうか.
	//
	// 注意：指定されたブロックIDのファイルは存在しているべきなのか？→Yes!なぜなら、ピンするということは、バッファーにファイルの内容を読み取るわけなので、Page をメモリに読み込むことになる！
	// というわけで、テスト用のファイルを用意しておいた方が良い。FileManager のメソッドをそのまま使えるだろう。

	t.Run("[条件]まだバッファーされていない [期待値]バッファーされている", func(t *testing.T) {
		testBuffer := bufferManager.Pin(firstBlockID)
		assert.True(t, bufferManager.bufferPool[0].IsPinned(), "Buffer[0] should be pinned")
		assert.True(t, !bufferManager.bufferPool[1].IsPinned(), "Buffer[1] should NOT be pinned")
		assert.True(t, !bufferManager.bufferPool[2].IsPinned(), "Buffer[2] should NOT be pinned")
		assert.Equal(t, types.Int(1), testBuffer.pinCount, "Pin count should be 1")
		assert.Equal(t, types.Int(2), bufferManager.numAvailable, "numAvailable should be 2")
	})

	t.Run("[条件]すでにバッファーされている＆ピンされている [期待値]ピンカウントが増えている", func(t *testing.T) {
		// すでにピンされているブロックをさらにピンする
		buffer := bufferManager.Pin(firstBlockID)

		assert.True(t, bufferManager.bufferPool[0].IsPinned(), "Buffer[0] should be pinned")
		assert.True(t, !bufferManager.bufferPool[1].IsPinned(), "Buffer[1] should NOT be pinned")
		assert.True(t, !bufferManager.bufferPool[2].IsPinned(), "Buffer[2] should NOT be pinned")
		assert.Equal(t, types.Int(2), buffer.pinCount, "Pin count should be 2")
		assert.Equal(t, types.Int(2), bufferManager.numAvailable, "numAvailable should be 2")
	})

	t.Run("[条件]すでにバッファーされている＆ピンされていない [期待値]ピンされている", func(t *testing.T) {
		// 2つピンされているので、2回unpinする
		pinnedBuffer := bufferManager.bufferPool[0]
		bufferManager.Unpin(pinnedBuffer)
		bufferManager.Unpin(pinnedBuffer)
		assert.True(t, !bufferManager.bufferPool[0].IsPinned(), "Buffer[0] should NOT be pinned")
		assert.True(t, !bufferManager.bufferPool[1].IsPinned(), "Buffer[1] should NOT be pinned")
		assert.True(t, !bufferManager.bufferPool[2].IsPinned(), "Buffer[2] should NOT be pinned")
		assert.Equal(t, types.Int(3), bufferManager.numAvailable, "numAvailable should be 3")

		// さらにピンする
		bufferManager.Pin(firstBlockID)
		assert.True(t, bufferManager.bufferPool[0].IsPinned(), "Buffer[0] should be pinned")
		assert.True(t, !bufferManager.bufferPool[1].IsPinned(), "Buffer[1] should NOT be pinned")
		assert.True(t, !bufferManager.bufferPool[2].IsPinned(), "Buffer[2] should NOT be pinned")
		assert.Equal(t, types.Int(2), bufferManager.numAvailable, "numAvailable should be 2")
	})

	t.Run("[条件]空いているバッファーがない場合にPinしようとし、タイムアウトを過ぎた [期待値]パニックする", func(t *testing.T) {
		// 空いている2つ目,3つ目のバッファーをピンしておき、バッファープールが埋まるようにする.
		bufferManager.Pin(secondBlockID)
		bufferManager.Pin(thirdBlockID)
		assert.True(t, bufferManager.bufferPool[0].IsPinned(), "Buffer[0] should be pinned")
		assert.True(t, bufferManager.bufferPool[1].IsPinned(), "Buffer[1] should be pinned")
		assert.True(t, bufferManager.bufferPool[2].IsPinned(), "Buffer[2] should be pinned")
		assert.Equal(t, types.Int(0), bufferManager.numAvailable, "numAvailable should be 0")

		// 追加でピンしようとする. 他の goroutine がアンピンしないので、タイムアウトする.
		assert.Panics(t, func() {
			bufferManager.Pin(forthBlockID)
		})
	})

	t.Run("[条件]空いているバッファーがない場合にPinしようとしたが、タイムアウト前にプールの空きができた [期待値]Pinできる", func(t *testing.T) {
		assert.True(t, bufferManager.bufferPool[0].IsPinned(), "Buffer[0] should be pinned")
		assert.True(t, bufferManager.bufferPool[1].IsPinned(), "Buffer[1] should be pinned")
		assert.True(t, bufferManager.bufferPool[2].IsPinned(), "Buffer[2] should be pinned")
		assert.Equal(t, types.Int(0), bufferManager.numAvailable, "numAvailable should be 0")
		assert.Len(t, bufferManager.waitList, 1, "len(bm.waitList) should be 1 (前のテストで1つ残っているため).")

		// 追加でピンしようとする.
		done := make(chan bool)
		go func() {
			defer close(done)
			pinnedBuffer := bufferManager.Pin(forthBlockID)
			if !assert.NotNil(t, pinnedBuffer, "Pin request should be successful.") {
				t.Errorf("Pin request should be successful. thirdBlockID=%+v\n", thirdBlockID)
			}
		}()

		// 別の goroutine でアンピンする
		done2 := make(chan bool)
		go func() {
			defer close(done2)
			time.Sleep(100 * time.Millisecond)
			bufferManager.Unpin(bufferManager.bufferPool[0])
		}()

		// 両方の goroutine が終了するまで待つ
		<-done
		<-done2

		assert.True(t, bufferManager.bufferPool[0].IsPinned(), "Buffer[0] should be pinned")
		assert.True(t, bufferManager.bufferPool[1].IsPinned(), "Buffer[1] should be pinned")
		assert.True(t, bufferManager.bufferPool[2].IsPinned(), "Buffer[2] should be pinned")
		assert.Equal(t, types.Int(0), bufferManager.numAvailable, "numAvailable should be 0")
	})
}
