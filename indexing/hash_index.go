package indexing

import (
	"fmt"
	"simple-db-go/query"
	"simple-db-go/record"
	"simple-db-go/transaction"
	"simple-db-go/types"
	"strconv"
)

var _ Index = (*HashIndex)(nil)

const bucketSize = 100

type HashIndex struct {
	transaction *transaction.Transaction
	indexName   types.IndexName
	layout      *record.Layout
	searchKey   query.Constant
	tableScan   *query.TableScan
}

func NewHashIndex(transaction *transaction.Transaction, indexName types.IndexName, layout *record.Layout) *HashIndex {
	return &HashIndex{
		transaction: transaction,
		indexName:   indexName,
		layout:      layout,
	}
}

func HashIndexSearchCost(numBlocks types.Int, recordsPerBlock types.Int) types.Int {
	return numBlocks / bucketSize
}

func (hi *HashIndex) BeforeFirst(searchKey query.Constant) {
	hi.Close()

	hi.searchKey = searchKey
	bucket := searchKey.HashCode() % bucketSize
	tableName := types.TableName(hi.indexName + types.IndexName(strconv.FormatUint(uint64(bucket), 10)))
	hi.tableScan = query.NewTableScan(hi.transaction, tableName, hi.layout)
}

func (hi *HashIndex) Next() bool {
	for hi.tableScan.Next() {
		value, err := hi.tableScan.GetValue("dataval")
		if err != nil {
			// インデックステーブルには`dataval`フィールドが必ず存在するので、ここは単にpanicする.
			panic(fmt.Sprintf("HashIndex.Next() で異常が発生. %s に dataval フィールドが含まれません.", hi.indexName))
		}

		if value == hi.searchKey {
			return true
		}
	}
	return false
}

func (hi *HashIndex) GetDataRecordID() record.RecordID {
	blockNumber, err := hi.tableScan.GetInt("block")
	if err != nil {
		// インデックステーブルには`block`フィールドが必ず存在するので、ここは単にpanicする.
		panic(fmt.Sprintf("HashIndex.GetDataRecordID() で異常が発生. %s に block フィールドが含まれません.", hi.indexName))
	}

	id, err := hi.tableScan.GetInt("id")
	if err != nil {
		// インデックステーブルには`id`フィールドが必ず存在するので、ここは単にpanicする.
		panic(fmt.Sprintf("HashIndex.GetDataRecordID() で異常が発生. %s に id フィールドが含まれません.", hi.indexName))
	}

	return record.NewRecordID(types.BlockNumber(blockNumber), types.SlotNumber(id))
}

func (hi *HashIndex) Insert(val query.Constant, dataRecordID record.RecordID) {
	hi.BeforeFirst(val)

	hi.tableScan.Insert()
	hi.tableScan.SetInt("block", types.Int(dataRecordID.GetBlockNumber()))
	hi.tableScan.SetInt("id", types.Int(dataRecordID.GetSlotNumber()))
	hi.tableScan.SetValue("dataval", val)
}

func (hi *HashIndex) Delete(val query.Constant, dataRecordID record.RecordID) {
	hi.BeforeFirst(val)

	for hi.Next() {
		if hi.GetDataRecordID() == dataRecordID {
			hi.tableScan.Delete()
			return
		}
	}
}

func (hi *HashIndex) Close() {
	if hi.tableScan != nil {
		hi.tableScan.Close()
	}
}
