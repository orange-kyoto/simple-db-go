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
	// Listen for connections on localhost port 4000
	l, err := net.Listen("tcp", "127.0.0.1:4000")
	if err != nil {
		logger.Error(err.Error())
		panic(err.Error())
	}

	// Accept a new connection once
	c, err := l.Accept()
	if err != nil {
		logger.Error(err.Error())
		panic(err.Error())
	}

	dbConfig := config.NewDBConfig()
	simpleDB := db.NewSimpleDB(dbConfig)

	// Create a connection with user root and an empty password.
	// You can use your own handler to handle command here.
	conn, err := server.NewConn(c, "root", "", simpleDBServer.NewHandler(dbConfig, simpleDB))
	if err != nil {
		logger.Error(err.Error())
		panic(err.Error())
	}

	logger.Info("SimpleDB サーバー起動！")

	// as long as the client keeps sending commands, keep handling them
	for {
		if err := conn.HandleCommand(); err != nil {
			logger.Error(err.Error())
			panic(err.Error())
		}
	}
}
