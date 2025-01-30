package server

import (
	"os"
	"simple-db-go/config"
	"simple-db-go/db"
	"simple-db-go/logger"
	"simple-db-go/planning"
	"simple-db-go/transaction"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
)

var _ server.Handler = (*SimpleDBSQLHandler)(nil)

type SimpleDBSQLHandler struct {
	dbConfig    config.DBConfig
	simpleDb    *db.SimpleDB
	transaction *transaction.Transaction
	planner     *planning.Planner
}

func NewHandler(dbConfig config.DBConfig, simpleDb *db.SimpleDB) *SimpleDBSQLHandler {
	queryPlanner := planning.NewBasicQueryPlanner(simpleDb.GetMetadataManager())
	updatePlanner := planning.NewBasicUpdatePlanner(simpleDb.GetMetadataManager())
	planner := planning.NewPlanner(queryPlanner, updatePlanner)
	transaction := simpleDb.NewTransaction()

	return &SimpleDBSQLHandler{dbConfig, simpleDb, transaction, planner}
}

func (h *SimpleDBSQLHandler) UseDB(dbName string) error {
	// dbConfig から、DBディレクトリを取得し、そのディレクトリが存在するか？だけチェックすればいい。
	dbDirectory := h.dbConfig.GetDBDirectory()

	_, err := os.Stat(dbDirectory)
	if os.IsNotExist(err) {
		return InvalidDBNameError{dbName}
	}

	return nil
}

// handle COM_QUERY command, like SELECT, INSERT, UPDATE, etc...
// If Result has a Resultset (SELECT, SHOW, etc...), we will send this as the response, otherwise, we will send Result
func (h *SimpleDBSQLHandler) HandleQuery(sql string) (*mysql.Result, error) {
	logger.Infof("HandleQuery: %s\n", sql)

	queryPlan, err1 := h.planner.CreateQueryPlan(sql, h.transaction)
	if err1 == nil {
		scan := queryPlan.Open()
		defer scan.Close()

		resultSet, err := buildResultsetFrom(scan)
		if err != nil {
			return nil, err
		}
		result := mysql.NewResult(resultSet)
		result.Status = mysql.SERVER_STATUS_IN_TRANS // AUTOCOMMIT=off がデフォルトの想定とし、接続時にはトランザクションが開始されるものとする.

		return result, nil
	} else {
		affectedRows, err2 := h.planner.ExecuteUpdate(sql, h.transaction)
		if err2 == nil {
			result := mysql.NewResult(nil)
			result.Status = mysql.SERVER_STATUS_IN_TRANS // AUTOCOMMIT=off がデフォルトの想定とする.
			result.AffectedRows = uint64(affectedRows)
			return result, nil
		} else {
			return nil, HandleQueryError{createQueryPlanError: err1, executeUpdateError: err2}
		}
	}
}

// handle COM_FILED_LIST command
func (h *SimpleDBSQLHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	logger.Infof("HandleFieldList: table=%s, fieldWildcard=%s\n", table, fieldWildcard)
	// テーブルのフィールド情報を返す処理を実装
	return nil, nil
}

// handle COM_STMT_PREPARE, params is the param number for this statement, columns is the column number
// context will be used later for statement execute
func (h *SimpleDBSQLHandler) HandleStmtPrepare(query string) (params int, columns int, context interface{}, err error) {
	logger.Infof("HandleStmtPrepare: %s\n", query)
	return 0, 0, nil, nil
}

// handle COM_STMT_EXECUTE, context is the previous one set in prepare
// query is the statement prepare query, and args is the params for this statement
func (h *SimpleDBSQLHandler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	logger.Infof("HandleStmtExecute: %s\n", query)
	// プリペアドステートメントの実行処理を実装
	result := new(mysql.Result)
	result.Status = mysql.SERVER_STATUS_AUTOCOMMIT
	return result, nil
}

// handle COM_STMT_CLOSE, context is the previous one set in prepare
// this handler has no response
func (h *SimpleDBSQLHandler) HandleStmtClose(context interface{}) error {
	logger.Info("HandleStmtClose\n")
	return nil
}

// handle any other command that is not currently handled by the library,
// default implementation for this method will return an ER_UNKNOWN_ERROR
func (h *SimpleDBSQLHandler) HandleOtherCommand(cmd byte, data []byte) error {
	logger.Infof("HandleOtherCommand: cmd=%d, data=%v\n", cmd, data)
	switch cmd {
	case mysql.COM_PING:
		logger.Info("Received COM_PING, responding with OK")
		return nil // MySQLクライアントが `ping` コマンドを送った場合にOKを返す
	case mysql.COM_QUIT:
		logger.Info("Received COM_QUIT, closing connection")
		return nil // クライアントの接続終了
	default:
		logger.Infof("Unknown command: %d\n", cmd)
	}
	return nil
}
