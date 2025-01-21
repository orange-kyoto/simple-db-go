package metadata

import (
	"simple-db-go/constants"
	"simple-db-go/query"
	"simple-db-go/record"
	"simple-db-go/transaction"
	"simple-db-go/types"
)

// Index のメタデータを管理する構造体.
type IndexManager struct {
	layout       *record.Layout
	tableManager *TableManager
	statManager  *StatManager
}

func NewIndexManager(isNew bool, tableManager *TableManager, statManager *StatManager, transaction *transaction.Transaction) *IndexManager {
	if isNew {
		schema := record.NewSchema()
		schema.AddStringField("index_name", constants.MAX_NAME_LENGTH)
		schema.AddStringField("table_name", constants.MAX_NAME_LENGTH)
		schema.AddStringField("field_name", constants.MAX_NAME_LENGTH)
		tableManager.CreateTable(INDEX_CATALOG_TABLE_NAME, schema, transaction)
	}

	indexCatalogLayout, err := tableManager.GetLayout(INDEX_CATALOG_TABLE_NAME, transaction)
	if err != nil {
		// インデックスカタログは必ず初期化するべきなので、無い場合は panic で落とす.
		panic("IndexManager の初期化時にインデックスカタログのレイアウトが取得できませんでした.")
	}

	return &IndexManager{
		layout:       indexCatalogLayout,
		tableManager: tableManager,
		statManager:  statManager,
	}
}

func (im *IndexManager) CreateIndex(indexName types.IndexName, tableName types.TableName, fieldName types.FieldName, transaction *transaction.Transaction) {
	tableScan := query.NewTableScan(transaction, INDEX_CATALOG_TABLE_NAME, im.layout)
	defer tableScan.Close()

	tableScan.Insert()
	tableScan.SetString("index_name", string(indexName))
	tableScan.SetString("table_name", string(tableName))
	tableScan.SetString("field_name", string(fieldName))
}

func (im *IndexManager) GetIndexInfo(tableName types.TableName, transaction *transaction.Transaction) (map[types.FieldName]*IndexInfo, error) {
	tableLayout, err := im.tableManager.GetLayout(tableName, transaction)
	if err != nil {
		return nil, CannotGetIndexInfoError{TableName: tableName, error: err}
	}

	result := make(map[types.FieldName]*IndexInfo)

	tableScan := query.NewTableScan(transaction, INDEX_CATALOG_TABLE_NAME, im.layout)
	defer tableScan.Close()

	for tableScan.Next() {
		row := ReadIndexCatalogRow(tableScan)

		if row.TableName == tableName {
			statInfo := im.statManager.GetStatInfo(tableName, tableLayout, transaction)
			indexInfo, err := NewIndexInfo(row.IndexName, row.FieldName, tableLayout.GetSchema(), transaction, statInfo)
			if err != nil {
				return nil, err
			}
			result[row.FieldName] = indexInfo
		}
	}

	return result, nil
}
