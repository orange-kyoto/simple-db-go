package metadata

import (
	"fmt"
	"simple-db-go/query"
	"simple-db-go/record"
	"simple-db-go/transaction"
	"simple-db-go/types"
	"sync"
)

type StatManager struct {
	tableManager *TableManager

	// StatManager は in-memory で統計情報を保持する.
	// また、この情報はリアルタイムで正確に更新は"しない".
	tableStats map[types.TableName]*StatInfo

	// 統計情報にアクセスされた回数を記録する.
	// 100 回を超えたタイミングで統計情報を更新する.
	numCalls types.Int

	// FileManager などとは方針を変えてみて、素直に sync を使ってみます.
	// Go言語のお勉強も目的なので、色々やってみたいというだけの理由です.
	mu sync.Mutex
}

// DB起動時に1度だけ呼ばれる.
func NewStatManager(tableManager *TableManager, transaction *transaction.Transaction) *StatManager {
	statManager := &StatManager{
		tableManager: tableManager,
		tableStats:   make(map[types.TableName]*StatInfo),
		numCalls:     0,
	}
	statManager.refreshStatistics(transaction)
	return statManager
}

func (sm *StatManager) GetStatInfo(tableName types.TableName, layout *record.Layout, transaction *transaction.Transaction) *StatInfo {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.numCalls++
	if sm.numCalls > 100 {
		sm.refreshStatistics(transaction)
	}

	statInfo := sm.tableStats[tableName]

	if statInfo == nil {
		statInfo = sm.calcTableStats(tableName, layout, transaction)
		sm.tableStats[tableName] = statInfo
	}

	return statInfo
}

func (sm *StatManager) refreshStatistics(transaction *transaction.Transaction) {
	sm.tableStats = make(map[types.TableName]*StatInfo)
	sm.numCalls = 0

	tableCatalogLayout := sm.tableManager.GetTableCatalogLayout()
	tableScan := query.NewTableScan(transaction, TABLE_CATALOG_TABLE_NAME, tableCatalogLayout)
	defer tableScan.Close()

	for tableScan.Next() {
		row := ReadTableCatalogRow(tableScan)

		layout, err := sm.tableManager.GetLayout(row.TableName, transaction)
		if err != nil {
			// 単にエラーログを出力するだけにする.
			fmt.Printf("統計情報の更新の際、テーブルのレイアウト取得に失敗しました. tableName=%s, err=%+v", row.TableName, err)
			continue
		}
		statInfo := sm.calcTableStats(row.TableName, layout, transaction)
		sm.tableStats[row.TableName] = statInfo
	}
}

func (sm *StatManager) calcTableStats(tableName types.TableName, layout *record.Layout, transaction *transaction.Transaction) *StatInfo {
	tableScan := query.NewTableScan(transaction, tableName, layout)
	defer tableScan.Close()

	numBlocks := types.Int(0)
	numRecords := types.Int(0)
	for tableScan.Next() {
		numRecords++
		numBlocks = types.Int(tableScan.GetCurrentRecordID().GetBlockNumber()) + 1
	}

	return NewStatInfo(numBlocks, numRecords)
}
