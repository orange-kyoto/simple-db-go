# SimpleDB Go

書籍[Database Design and Implementation: Second Edition](https://amzn.asia/d/hSucaKW)を Go 言語で実装する筆者学習用プロジェクトです。

## 当プロジェクトの目的

1. データベースの基本的な概念を理解する。
2. Go 言語に入門し、基礎的な内容を習得する。

## SimpleDB サーバーの起動

例：

```
$ SIMPLE_DB_DIRECTORY=dev \
  SIMPLE_DB_LOG_FILE_NAME=devlog \
  SIMPLE_DB_BLOCK_SIZE=4096 \
  SIMPLE_DB_BUFFER_POOL_SIZE=10 \
  go run ./main.go
```

mysql クライアントから接続することができます。

```
$ mysql -h127.0.0.1 -P4000 -uroot
...
mysql>
```
