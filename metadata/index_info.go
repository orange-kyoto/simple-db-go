package metadata

import (
	"simple-db-go/constants"
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
) *IndexInfo {
	indexInfo := &IndexInfo{
		indexName:   indexName,
		fieldName:   fieldName,
		transaction: transaction,
		tableSchema: tableSchema,
		statInfo:    statInfo,
	}

	indexInfo.indexLayout = indexInfo.createIndexLayout()

	return indexInfo
}

// インデックスを捜索するのに必要なブロックアクセス数.
// 検索のためのコストを計算するためのメソッド.
func (ii *IndexInfo) GetBlocksAccessed() types.Int {
	// TODO: HashIndex が実装されたら、そのコストを計算する. Chapter12.
	return -1
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

// TODO: Chapter12 で実装する.
// Index struct を返すメソッドのようだ。
// TODO なので、Index struct は定義していない.
func (ii *IndexInfo) Open() {}

// インデックスのレイアウトを計算する.
func (ii *IndexInfo) createIndexLayout() *record.Layout {
	indexSchema := record.NewSchema()
	indexSchema.AddIntField("block")
	indexSchema.AddIntField("id")

	if ii.tableSchema.FieldType(ii.fieldName) == constants.INTEGER {
		indexSchema.AddIntField("data_val")
	} else {
		fieldLength := ii.tableSchema.Length(ii.fieldName)
		indexSchema.AddStringField("data_val", fieldLength)
	}
	return record.NewLayout(indexSchema)
}
