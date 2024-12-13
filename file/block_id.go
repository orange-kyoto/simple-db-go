package file

import (
	"hash/fnv"
	"strconv"
)

type BlockID struct {
	Filename string
	Blknum   int
}

func NewBlockID(filename string, blknum int) BlockID {
	return BlockID{Filename: filename, Blknum: blknum}
}

func (b BlockID) Equals(other BlockID) bool {
	return b.Filename == other.Filename && b.Blknum == other.Blknum
}

func (b BlockID) ToString() string {
	return "[file " + b.Filename + ", block " + strconv.Itoa(b.Blknum) + "]"
}

func (b BlockID) HashCode() int {
	hasher := fnv.New32a()
	hasher.Write([]byte(b.ToString()))
	return int(hasher.Sum32())
}
