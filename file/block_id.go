package file

import (
	"fmt"
	"hash/fnv"
	"simple-db-go/types"
)

type BlockID struct {
	Filename string
	Blknum   types.Int
}

func NewBlockID(filename string, blknum types.Int) *BlockID {
	return &BlockID{Filename: filename, Blknum: blknum}
}

func (b BlockID) Equals(other BlockID) bool {
	return b.Filename == other.Filename && b.Blknum == other.Blknum
}

func (b BlockID) ToString() string {
	return fmt.Sprintf("[file %s, block %d]", b.Filename, b.Blknum)
}

func (b BlockID) HashCode() int {
	hasher := fnv.New32a()
	hasher.Write([]byte(b.ToString()))
	return int(hasher.Sum32())
}
