package transaction

import (
	"simple-db-go/constants"
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/types"
)

type CheckpointRecord struct{}

/*
# CHECKPOINT レコードの構造

```example
<CHECKPOINT>
```

- 他の情報はなし
*/
func NewCheckpointRecord() *CheckpointRecord {
	return &CheckpointRecord{}
}

func (cr *CheckpointRecord) GetOperation() RecordOperator {
	return CHECKPOINT
}

func (cr *CheckpointRecord) GetTransactionNumber() types.TransactionNumber {
	return DummyTransactionNumber
}

func (cr *CheckpointRecord) Undo(t *Transaction) {
	// することがないので何もしない
}

func (cr *CheckpointRecord) ToString() string {
	return "<CHECKPOINT>"
}

func WriteCheckpointRecord(logManager *log.LogManager) log.LSN {
	recordLength := constants.Int32ByteSize
	rawLogRecord := make([]byte, recordLength)

	page := file.NewPageFrom(rawLogRecord)
	page.SetInt(0, types.Int(CHECKPOINT))

	return logManager.Append(page.Data)
}
