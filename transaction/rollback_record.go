package transaction

import (
	"fmt"
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/types"
)

type RollbackRecord struct {
	transactionNumber types.TransactionNumber
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
	txNum := types.TransactionNumber(page.GetInt(tpos))

	return &RollbackRecord{
		transactionNumber: txNum,
	}
}

func (rr *RollbackRecord) GetOperation() RecordOperator {
	return ROLLBACK
}

func (rr *RollbackRecord) GetTransactionNumber() types.TransactionNumber {
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

func WriteRollbackRecord(logManager *log.LogManager, transactionNumber types.TransactionNumber) log.LSN {
	tpos := file.Int32ByteSize
	recordLength := tpos + file.Int32ByteSize

	rawLogRecord := make([]byte, 0, recordLength)
	page := file.NewPageFrom(rawLogRecord)
	page.SetInt(0, types.Int(ROLLBACK))
	page.SetInt(tpos, types.Int(transactionNumber))

	return logManager.Append(page.Data)
}
