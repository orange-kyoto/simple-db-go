package planning

import (
	"simple-db-go/parsing/data"
	"simple-db-go/transaction"
	"simple-db-go/types"
)

// 全てのメソッドで、影響のあったレコード数を返す.
type UpdatePlanner interface {
	ExecuteInsert(data *data.InsertData, transaction *transaction.Transaction) (types.Int, error)
	ExecuteDelete(data *data.DeleteData, transaction *transaction.Transaction) (types.Int, error)
	ExecuteModify(data *data.ModifyData, transaction *transaction.Transaction) (types.Int, error)
	ExecuteCreateTable(data *data.CreateTableData, transaction *transaction.Transaction) types.Int
	ExecuteCreateView(data *data.CreateViewData, transaction *transaction.Transaction) types.Int
	ExecuteCreateIndex(data *data.CreateIndexData, transaction *transaction.Transaction) types.Int
}
