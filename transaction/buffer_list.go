package transaction

import (
	"simple-db-go/buffer"
	"simple-db-go/file"
	"simple-db-go/types"
)

// トランザクションごとに用意する.
// そのトランザクションが pin しているバッファーを管理する構造体.
type BufferList struct {
	// トランザクションがアクセスするバッファーがどのブロックでPinされているかを管理する
	buffers map[file.BlockID]*buffer.Buffer
	// 各ブロックが pin されている個数を管理する.
	pins map[file.BlockID]types.Int

	bufferManager *buffer.BufferManager
}

func NewBufferList(bm *buffer.BufferManager) *BufferList {
	return &BufferList{
		buffers:       make(map[file.BlockID]*buffer.Buffer),
		pins:          make(map[file.BlockID]types.Int),
		bufferManager: bm,
	}
}

func (bl *BufferList) GetBuffer(blockID file.BlockID) *buffer.Buffer {
	return bl.buffers[blockID]
}

func (bl *BufferList) Pin(blockID file.BlockID) {
	buffer := bl.bufferManager.Pin(blockID)
	bl.buffers[blockID] = buffer
	bl.pins[blockID] += 1
}

func (bl *BufferList) Unpin(blockID file.BlockID) {
	buffer := bl.buffers[blockID]
	bl.bufferManager.Unpin(buffer)
	bl.pins[blockID] -= 1
	if bl.pins[blockID] == 0 {
		delete(bl.buffers, blockID)
	}
}

func (bl *BufferList) UnpinAll() {
	for blockID := range bl.buffers {
		buffer := bl.buffers[blockID]
		bl.bufferManager.Unpin(buffer)
	}
	bl.buffers = make(map[file.BlockID]*buffer.Buffer)
	bl.pins = make(map[file.BlockID]types.Int)
}
