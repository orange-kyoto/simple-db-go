package metadata

import (
	"fmt"
	"simple-db-go/constants"
	"simple-db-go/record"
	"simple-db-go/transaction"
	"simple-db-go/types"
)

type ViewManager struct {
	tableManager *TableManager
}

func NewViewManager(isNew bool, tableManager *TableManager, transaction *transaction.Transaction) *ViewManager {
	viewManager := &ViewManager{
		tableManager: tableManager,
	}

	if isNew {
		schema := record.NewSchema()
		schema.AddStringField("view_name", constants.MAX_NAME_LENGTH)
		schema.AddStringField("view_def", constants.MAX_VIEW_DEF_LENGTH)
		tableManager.CreateTable(VIEW_CATALOG_TABLE_NAME, schema, transaction)
	}

	return viewManager
}

func (vm *ViewManager) CreateView(viewName types.ViewName, viewDef types.ViewDef, transaction *transaction.Transaction) {
	layout, err := vm.tableManager.GetLayout(VIEW_CATALOG_TABLE_NAME, transaction)
	if err != nil {
		// 初期起動時に必ずカタログのレイアウトが登録されているはずなので、ここは panic にしておく.
		panic(fmt.Sprintf("ビューの作成に失敗しました. err=%+v", err))
	}

	tableScan := record.NewTableScan(transaction, VIEW_CATALOG_TABLE_NAME, layout)
	defer tableScan.Close()

	tableScan.Insert()
	tableScan.SetString("view_name", string(viewName))
	tableScan.SetString("view_def", string(viewDef))
}

func (vm *ViewManager) GetViewDef(viewName types.ViewName, transaction *transaction.Transaction) (types.ViewDef, error) {
	layout, err := vm.tableManager.GetLayout(VIEW_CATALOG_TABLE_NAME, transaction)
	if err != nil {
		// 初期起動時に必ずカタログのレイアウトが登録されているはずなので、ここは panic にしておく.
		panic(fmt.Sprintf("ビューの取得に失敗しました. err=%+v", err))
	}

	tableScan := record.NewTableScan(transaction, VIEW_CATALOG_TABLE_NAME, layout)
	defer tableScan.Close()

	for tableScan.Next() {
		if tableScan.GetString("view_name") == string(viewName) {
			viewDef := types.ViewDef(tableScan.GetString("view_def"))
			return viewDef, nil
		}
	}

	return "", CannotGetViewError{ViewName: viewName, error: fmt.Errorf("View not found. view_name=%s", viewName)}
}
