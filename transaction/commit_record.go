package transaction

import (
	"fmt"
	"simple-db-go/file"
	"simple-db-go/log"
)

type CommitRecord struct {
	transactionNumber TransactionNumber
}

/*
# COMMIT レコードの構造

```example
<COMMIT 0>
```

* 1つ目：COMMIT
* 2つ目：トランザクション番号
*/
func NewCommitRecord(page *file.Page) *CommitRecord {
	tpos := file.Int32ByteSize
	txNum := TransactionNumber(page.GetInt(tpos))

	return &CommitRecord{
		transactionNumber: txNum,
	}
}

func (cr *CommitRecord) GetOperation() RecordOperator {
	return COMMIT
}

func (cr *CommitRecord) GetTransactionNumber() TransactionNumber {
	return cr.transactionNumber
}

func (cr *CommitRecord) Undo(t *Transaction) {
	// することがないので何もしない
}

func (cr *CommitRecord) ToString() string {
	return fmt.Sprintf(
		"<COMMIT %d>",
		cr.transactionNumber,
	)
}

func WriteCommitRecord(logManager *log.LogManager, transactionNumber TransactionNumber) log.LSN {
	tpos := file.Int32ByteSize
	recordLength := tpos + file.Int32ByteSize

	rawLogRecord := make([]byte, 0, recordLength)
	page := file.NewPageFrom(rawLogRecord)
	page.SetInt(0, int32(COMMIT))
	page.SetInt(tpos, int32(transactionNumber))

	return logManager.Append(page.Data)
}
