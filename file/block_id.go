package file

import (
	"fmt"
	"hash/fnv"
	"simple-db-go/types"
)

type BlockID struct {
	Filename    string
	BlockNumber types.BlockNumber
}

func NewBlockID(filename string, blknum types.BlockNumber) *BlockID {
	return &BlockID{Filename: filename, BlockNumber: blknum}
}

func (b BlockID) Equals(other BlockID) bool {
	return b.Filename == other.Filename && b.BlockNumber == other.BlockNumber
}

func (b BlockID) ToString() string {
	return fmt.Sprintf("[file %s, block %d]", b.Filename, b.BlockNumber)
}

func (b BlockID) HashCode() int {
	hasher := fnv.New32a()
	hasher.Write([]byte(b.ToString()))
	return int(hasher.Sum32())
}
