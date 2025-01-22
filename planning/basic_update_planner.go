package planning

import (
	"simple-db-go/metadata"
	"simple-db-go/parsing/data"
	"simple-db-go/query"
	"simple-db-go/transaction"
	"simple-db-go/types"
)

var _ UpdatePlanner = (*BasicUpdatePlanner)(nil)

type BasicUpdatePlanner struct {
	metadataManager *metadata.MetadataManager
}

func NewBasicUpdatePlanner(metadataManager *metadata.MetadataManager) *BasicUpdatePlanner {
	return &BasicUpdatePlanner{metadataManager: metadataManager}
}

func (up *BasicUpdatePlanner) ExecuteDelete(deleteData *data.DeleteData, transaction *transaction.Transaction) (types.Int, error) {
	plan, err := NewTablePlan(transaction, deleteData.TableName, up.metadataManager)
	if err != nil {
		return 0, err
	}

	plan = NewSelectPlan(plan, deleteData.Predicate)

	// NOTE: テーブル名であることは、NewTablePlanが成功していることからわかる.
	// 尚且つ、SelectPlan(SelectScan)では、Updatable であることは変わらないので、キャストして問題ない.
	updateScan := plan.Open().(query.UpdateScan)
	defer updateScan.Close()

	count := types.Int(0)
	for updateScan.Next() {
		updateScan.Delete()
		count++
	}

	return count, nil
}

func (up *BasicUpdatePlanner) ExecuteModify(modifyData *data.ModifyData, transaction *transaction.Transaction) (types.Int, error) {
	plan, err := NewTablePlan(transaction, modifyData.TableName, up.metadataManager)
	if err != nil {
		return 0, err
	}

	plan = NewSelectPlan(plan, modifyData.Predicate)

	// NOTE: テーブル名であることは、NewTablePlanが成功していることからわかる.
	// 尚且つ、SelectPlan(SelectScan)では、Updatable であることは変わらないので、キャストして問題ない.
	updateScan := plan.Open().(query.UpdateScan)
	defer updateScan.Close()

	count := types.Int(0)
	for updateScan.Next() {
		newValue, err := modifyData.NewValue.Evaluate(updateScan)
		if err != nil {
			return 0, err
		}
		updateScan.SetValue(modifyData.FieldName, newValue)
		count++
	}

	return count, nil
}

func (up *BasicUpdatePlanner) ExecuteInsert(insertData *data.InsertData, transaction *transaction.Transaction) (types.Int, error) {
	plan, err := NewTablePlan(transaction, insertData.TableName, up.metadataManager)
	if err != nil {
		return 0, err
	}

	updateScan := plan.Open().(query.UpdateScan)
	defer updateScan.Close()

	updateScan.Insert()
	for i, fieldName := range insertData.FieldNames {
		value := insertData.Values[i]
		updateScan.SetValue(fieldName, value)
	}

	return 1, nil
}

func (up *BasicUpdatePlanner) ExecuteCreateTable(createTableData *data.CreateTableData, transaction *transaction.Transaction) types.Int {
	up.metadataManager.CreateTable(createTableData.TableName, createTableData.Schema, transaction)
	return 0
}

func (up *BasicUpdatePlanner) ExecuteCreateView(createViewData *data.CreateViewData, transaction *transaction.Transaction) types.Int {
	up.metadataManager.CreateView(createViewData.ViewName, createViewData.GetViewDef(), transaction)
	return 0
}

func (up *BasicUpdatePlanner) ExecuteCreateIndex(createIndexData *data.CreateIndexData, transaction *transaction.Transaction) types.Int {
	up.metadataManager.CreateIndex(createIndexData.IndexName, createIndexData.TableName, createIndexData.FieldName, transaction)
	return 0
}
