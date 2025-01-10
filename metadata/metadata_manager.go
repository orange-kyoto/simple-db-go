package metadata

import (
	"simple-db-go/record"
	"simple-db-go/transaction"
	"simple-db-go/types"
)

type MetadataManager struct {
	tableManager *TableManager
	viewManager  *ViewManager
	statManager  *StatManager
	indexManager *IndexManager
}

func NewMetadataManager(isNew bool, transaction *transaction.Transaction) *MetadataManager {
	tableManager := NewTableManager(isNew, transaction)
	viewManager := NewViewManager(isNew, tableManager, transaction)
	statManager := NewStatManager(tableManager, transaction)
	indexManager := NewIndexManager(isNew, tableManager, statManager, transaction)

	return &MetadataManager{
		tableManager: tableManager,
		viewManager:  viewManager,
		statManager:  statManager,
		indexManager: indexManager,
	}
}

func (mm *MetadataManager) CreateTable(tableName types.TableName, schema *record.Schema, transaction *transaction.Transaction) {
	mm.tableManager.CreateTable(tableName, schema, transaction)
}

func (mm *MetadataManager) GetLayout(tableName types.TableName, transaction *transaction.Transaction) (*record.Layout, error) {
	return mm.tableManager.GetLayout(tableName, transaction)
}

func (mm *MetadataManager) CreateView(viewName types.ViewName, viewDef types.ViewDef, transaction *transaction.Transaction) {
	mm.viewManager.CreateView(viewName, viewDef, transaction)
}

func (mm *MetadataManager) GetViewDef(viewName types.ViewName, transaction *transaction.Transaction) (types.ViewDef, error) {
	return mm.viewManager.GetViewDef(viewName, transaction)
}

func (mm *MetadataManager) CreateIndex(indexName types.IndexName, tableName types.TableName, fieldName types.FieldName, transaction *transaction.Transaction) {
	mm.indexManager.CreateIndex(indexName, tableName, fieldName, transaction)
}

func (mm *MetadataManager) GetIndexInfo(tableName types.TableName, transaction *transaction.Transaction) (map[types.FieldName]*IndexInfo, error) {
	return mm.indexManager.GetIndexInfo(tableName, transaction)
}

func (mm *MetadataManager) GetStatInfo(tableName types.TableName, layout *record.Layout, transaction *transaction.Transaction) *StatInfo {
	return mm.statManager.GetStatInfo(tableName, layout, transaction)
}
