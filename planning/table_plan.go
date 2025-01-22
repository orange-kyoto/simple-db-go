package planning

import (
	"simple-db-go/metadata"
	"simple-db-go/query"
	"simple-db-go/record"
	"simple-db-go/transaction"
	"simple-db-go/types"
)

var _ query.Plan = (*TablePlan)(nil)

type TablePlan struct {
	transaction *transaction.Transaction
	tableName   types.TableName
	layout      *record.Layout
	statInfo    *metadata.StatInfo
}

func NewTablePlan(transaction *transaction.Transaction, tableName types.TableName, metadataManager *metadata.MetadataManager) (query.Plan, error) {
	layout, err := metadataManager.GetLayout(tableName, transaction)
	if err != nil {
		return nil, err
	}

	statInfo := metadataManager.GetStatInfo(tableName, layout, transaction)

	return &TablePlan{
		transaction: transaction,
		tableName:   tableName,
		layout:      layout,
		statInfo:    statInfo,
	}, nil
}

func (p *TablePlan) Open() query.Scan {
	return query.NewTableScan(p.transaction, p.tableName, p.layout)
}

func (p *TablePlan) GetBlocksAccessed() types.Int {
	return p.statInfo.GetBlocksAccessed()
}

func (p *TablePlan) GetRecordsOutput() types.Int {
	return p.statInfo.GetRecordsOutput()
}

func (p *TablePlan) GetDistinctValues(fieldName types.FieldName) types.Int {
	return p.statInfo.GetDistinctValues(fieldName)
}

func (p *TablePlan) GetSchema() *record.Schema {
	return p.layout.GetSchema()
}
