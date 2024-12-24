package transaction

import (
	"fmt"
	"simple-db-go/file"
	"simple-db-go/log"
)

type RollbackRecord struct {
	transactionNumber TransactionNumber
}

/*
# ROLLBACK レコードの構造

```example
<ROLLBACK 0>
```

* 1つ目：ROLLBACK
* 2つ目：トランザクション番号
*/
func NewRollbackRecord(page *file.Page) *RollbackRecord {
	tpos := file.Int32ByteSize
	txNum := TransactionNumber(page.GetInt(tpos))

	return &RollbackRecord{
		transactionNumber: txNum,
	}
}

func (rr *RollbackRecord) GetOperation() RecordOperator {
	return ROLLBACK
}

func (rr *RollbackRecord) GetTransactionNumber() TransactionNumber {
	return rr.transactionNumber
}

func (rr *RollbackRecord) Undo(t *Transaction) {
	// することがないので何もしない
}

func (rr *RollbackRecord) ToString() string {
	return fmt.Sprintf(
		"<ROLLBACK %d>",
		rr.transactionNumber,
	)
}

func WriteRollbackRecord(logManager *log.LogManager, transactionNumber TransactionNumber) log.LSN {
	tpos := file.Int32ByteSize
	recordLength := tpos + file.Int32ByteSize

	rawLogRecord := make([]byte, 0, recordLength)
	page := file.NewPageFrom(rawLogRecord)
	page.SetInt(0, int32(ROLLBACK))
	page.SetInt(tpos, int32(transactionNumber))

	return logManager.Append(page.Data)
}
