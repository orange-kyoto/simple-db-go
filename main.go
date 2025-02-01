package main

import (
	"net"
	"simple-db-go/config"
	"simple-db-go/db"
	"simple-db-go/logger"
	simpleDBServer "simple-db-go/server"

	"github.com/go-mysql-org/go-mysql/server"
)

func main() {
	dbConfig := config.NewDBConfig()
	simpleDB := db.NewSimpleDB(dbConfig)

	// Listen for connections on localhost port 4000
	l, err := net.Listen("tcp", "127.0.0.1:4000")
	if err != nil {
		logger.Error(err.Error())
		panic(err.Error())
	}
	logger.Info("SimpleDB サーバー起動！")
	defer l.Close()

	svr := server.NewDefaultServer()
	cp := simpleDBServer.NewCredentialProvider()

	for {
		c, err := l.Accept()
		if err != nil {
			logger.Error(err.Error())
			panic(err.Error())
		}
		logger.Info("接続を受け付けました\n")

		go func(c net.Conn) {
			defer c.Close()

			conn, err := server.NewCustomizedConn(c, svr, cp, simpleDBServer.NewHandler(dbConfig, simpleDB))
			if err != nil {
				logger.Error(err.Error())
				panic(err.Error())
			}
			logger.Infof("[connection_id:%d] DB接続を確立しました.\n", conn.ConnectionID())

			for {
				if err := conn.HandleCommand(); err != nil {
					logger.Error(err.Error())
				} else {
					logger.Infof("[connection_id:%d] SQLの処理が成功しました.\n", conn.ConnectionID())
				}
			}
		}(c)
	}
}
