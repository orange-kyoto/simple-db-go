package metadata

import (
	"simple-db-go/constants"
	"simple-db-go/indexing"
	"simple-db-go/record"
	"simple-db-go/transaction"
	"simple-db-go/types"
)

// Index の統計情報.
type IndexInfo struct {
	indexName   types.IndexName
	fieldName   types.FieldName
	transaction *transaction.Transaction
	tableSchema *record.Schema
	indexLayout *record.Layout
	statInfo    *StatInfo
}

func NewIndexInfo(
	indexName types.IndexName,
	fieldName types.FieldName,
	tableSchema *record.Schema,
	transaction *transaction.Transaction,
	statInfo *StatInfo,
) (*IndexInfo, error) {
	indexInfo := &IndexInfo{
		indexName:   indexName,
		fieldName:   fieldName,
		transaction: transaction,
		tableSchema: tableSchema,
		statInfo:    statInfo,
	}

	indexLayout, err := indexInfo.createIndexLayout()
	if err != nil {
		return nil, err
	}

	indexInfo.indexLayout = indexLayout
	return indexInfo, nil
}

// インデックスを捜索するのに必要なブロックアクセス数.
// 検索のためのコストを計算するためのメソッド.
// NOTE: HashIndex のコストを計算している（BTree に変えることもできる）
func (ii *IndexInfo) GetBlocksAccessed() types.Int {
	recordsPerBlock := ii.transaction.BlockSize() / types.Int(ii.indexLayout.GetSlotSize())
	numBlocks := ii.statInfo.GetRecordsOutput() / recordsPerBlock
	return indexing.HashIndexSearchCost(numBlocks, recordsPerBlock)
}

// インデックスに存在するレコードの数.
// つまり、インデックスされたフィールドの各値に対して、幾つレコードが存在するか？を推定するためのメソッドか.
func (ii *IndexInfo) GetRecordsOutput() types.Int {
	return ii.statInfo.GetRecordsOutput() / ii.statInfo.GetDistinctValues(ii.fieldName)
}

// インデックスに存在する異なる値の数.
// インデックスされたフィールドについては、それぞれの値しか知らないので１を返す.
// 当該インデックスが対象としていないフィールドについては単にテーブルと同じ統計情報を返す.
func (ii *IndexInfo) GetDistinctValues(fieldName types.FieldName) types.Int {
	if ii.fieldName == fieldName {
		// これなんで１なんだろう？
		return 1
	} else {
		return ii.statInfo.GetDistinctValues(fieldName)
	}
}

func (ii *IndexInfo) Open() indexing.Index {
	return indexing.NewHashIndex(ii.transaction, ii.indexName, ii.indexLayout)
}

// インデックスのレイアウトを計算する.
func (ii *IndexInfo) createIndexLayout() (*record.Layout, error) {
	indexSchema := record.NewSchema()
	indexSchema.AddIntField("block")
	indexSchema.AddIntField("id")

	fieldType, err := ii.tableSchema.FieldType(ii.fieldName)
	if err != nil {
		return nil, err
	}

	if fieldType == constants.INTEGER {
		indexSchema.AddIntField("dataval")
	} else {
		fieldLength, err := ii.tableSchema.Length(ii.fieldName)
		if err != nil {
			return nil, err
		}
		indexSchema.AddStringField("dataval", fieldLength)
	}
	return record.NewLayout(indexSchema), nil
}
