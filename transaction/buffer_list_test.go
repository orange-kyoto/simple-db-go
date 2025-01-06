package transaction

import (
	"simple-db-go/buffer"
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	testDirForBufferList     = "test_buffer_list"
	logFileNameForBufferList = "test_buffer_list.log"
	blockSizeForBufferList   = 512
)

func startBufferList() (*file.FileManager, *buffer.BufferManager, *BufferList) {
	fileManager := file.NewFileManager(testDirForBufferList, blockSizeForBufferList)
	logManager := log.NewLogManager(fileManager, logFileNameForBufferList)
	bufferManager := buffer.NewBufferManager(fileManager, logManager, 3)
	bufferList := NewBufferList(bufferManager)

	return fileManager, bufferManager, bufferList
}

func TestPinUnpinBuffers(t *testing.T) {
	fileManager, _, bufferList := startBufferList()

	// Pin するためのファイルを用意し、ブロックを追加しておく。
	testFileName := "test_buffer_list.data"
	fileManager.Append(testFileName)
	fileManager.Append(testFileName)

	t.Run("正常に Pin できる.", func(t *testing.T) {
		blockID1 := file.NewBlockID("test_buffer_list.data", 0)
		bufferList.Pin(blockID1)
		buffer1 := bufferList.GetBuffer(blockID1)
		assert.NotNil(t, buffer1, "Pin できるのでバッファーが取得できる.")
		assert.Equal(t, types.Int(1), bufferList.pins[*blockID1], "Pin できるので pins には 1 が格納されている.")
		assert.True(t, buffer1.IsPinned(), "Pin できるのでバッファーはピンされている.")

		// もう一度 Pin した時に、同じバファーにPinされていることを確認する.
		blockID2 := file.NewBlockID("test_buffer_list.data", 0)
		bufferList.Pin(blockID2)
		buffer2 := bufferList.GetBuffer(blockID2)
		assert.Same(t, buffer1, buffer2, "同じブロックに対して Pin すると同じバッファーが返される.")
		assert.Equal(t, types.Int(2), bufferList.pins[*blockID1], "Pin できるので pins には 2 が格納されている.")
		assert.True(t, buffer2.IsPinned(), "Pin できるのでバッファーはピンされている.")
	})

	t.Run("正常に Unpin できる.", func(t *testing.T) {
		// 前のテストでピンしたブロックをアンピンする.
		blockID := file.NewBlockID("test_buffer_list.data", 0)
		bufferList.Unpin(blockID)
		buffer1 := bufferList.GetBuffer(blockID)
		assert.NotNil(t, buffer1, "2回 Pin した後に1回 Unpin してもバッファーは取得できる.")
		assert.Equal(t, types.Int(1), bufferList.pins[*blockID], "2回 Pin した後に1回 Unpin すると pins には 1 が格納されている.")
		assert.True(t, buffer1.IsPinned(), "2回 Pin した後に1回 Unpin するとバッファーはピンされている.")

		// もう一度 Unpin した時に、バッファーが削除されることを確認する.
		bufferList.Unpin(blockID)
		buffer2 := bufferList.GetBuffer(blockID)
		assert.Nil(t, buffer2, "2回 Pin した後に2回 Unpin するとバッファーは取得できない.")
		assert.Equal(t, types.Int(0), bufferList.pins[*blockID], "2回 Pin した後に2回 Unpin すると pins には 0 が格納されている.")
		assert.False(t, buffer1.IsPinned(), "2回 Pin した後に2回 Unpin するとバッファーはピンされていない.")
	})

	t.Run("正常に UnpinAll できる.", func(t *testing.T) {
		blockID1 := file.NewBlockID("test_buffer_list.data", 0)
		blockID2 := file.NewBlockID("test_buffer_list.data", 1)
		bufferList.Pin(blockID1)
		bufferList.Pin(blockID2)
		buffer1 := bufferList.GetBuffer(blockID1)
		buffer2 := bufferList.GetBuffer(blockID2)

		bufferList.UnpinAll()

		assert.Len(t, bufferList.buffers, 0, "UnpinAll すると buffers は空になる.")
		assert.Len(t, bufferList.pins, 0, "UnpinAll すると pins は空になる.")
		assert.False(t, buffer1.IsPinned(), "UnpinAll するとバッファーはピンされていない.")
		assert.False(t, buffer2.IsPinned(), "UnpinAll するとバッファーはピンされていない.")
	})
}
