package transaction

import (
	"simple-db-go/file"
)

type RecordOperator int32

type TransactionNumber int32

const (
	CHECKPOINT RecordOperator = iota
	START
	COMMIT
	ROLLBACK
	SETINT
	SETSTRING
)

const DummyTransactionNumber TransactionNumber = -1

type LogRecord interface {
	GetOperation() RecordOperator
	GetTransactionNumber() TransactionNumber
	Undo(*Transaction)
}

func CreateLogRecord(b []byte) LogRecord {
	page := file.NewPageFrom(b)

	switch RecordOperator(page.GetInt(0)) {
	case CHECKPOINT:
		return NewCheckpointRecord()
	case START:
		return NewStartRecord(page)
	case COMMIT:
		return NewCommitRecord(page)
	case ROLLBACK:
		return NewRollbackRecord(page)
	case SETINT:
		return NewSetIntRecord(page)
	case SETSTRING:
		return NewSetStringRecord(page)
	}
}
