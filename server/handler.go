package server

import (
	"os"
	"simple-db-go/config"
	"simple-db-go/db"
	"simple-db-go/logger"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
)

var _ server.Handler = (*SimpleDBSQLHandler)(nil)

type SimpleDBSQLHandler struct {
	dbConfig config.DBConfig
	simpleDb *db.SimpleDB
}

func NewHandler(dbConfig config.DBConfig, simpleDb *db.SimpleDB) *SimpleDBSQLHandler {
	return &SimpleDBSQLHandler{dbConfig, simpleDb}
}

// DB が存在するかどうかチェックすれば良さそうやな。
// 今回は起動時に1つだけDBがある想定なので、それに一致するかどうか？で判断すればいい。
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
func (h *SimpleDBSQLHandler) HandleQuery(query string) (*mysql.Result, error) {
	// これが一番大事そうかつ、これがやりたかったこと！
	// 試しに、いつでも固定の値を返す処理を入れてみるか？

	logger.Infof("HandleQuery: %s\n", query)
	result := new(mysql.Result)
	result.Status = mysql.SERVER_STATUS_AUTOCOMMIT
	return result, nil
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
