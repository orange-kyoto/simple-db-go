package metadata

import (
	"simple-db-go/types"
)

// テーブルの統計情報
type StatInfo struct {
	numBlocks  types.Int
	numRecords types.Int
}

func NewStatInfo(numBLocks types.Int, numRecords types.Int) *StatInfo {
	return &StatInfo{
		numBlocks:  numBLocks,
		numRecords: numRecords,
	}
}

func (si *StatInfo) GetBlocksAccessed() types.Int {
	return si.numBlocks
}

func (si *StatInfo) GetRecordsOutput() types.Int {
	return si.numRecords
}

// NOTE: 雑な推定に基づいて値を返している.
// Exercice 7.12 で改善する.
func (si *StatInfo) GetDistinctValues(fieldName types.FieldName) types.Int {
	return 1 + (si.numRecords / 3)
}
