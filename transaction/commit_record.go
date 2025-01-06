package transaction

import (
	"fmt"
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/types"
)

type CommitRecord struct {
	transactionNumber types.TransactionNumber
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
	txNum := types.TransactionNumber(page.GetInt(tpos))

	return &CommitRecord{
		transactionNumber: txNum,
	}
}

func (cr *CommitRecord) GetOperation() RecordOperator {
	return COMMIT
}

func (cr *CommitRecord) GetTransactionNumber() types.TransactionNumber {
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

func WriteCommitRecord(logManager *log.LogManager, transactionNumber types.TransactionNumber) log.LSN {
	tpos := file.Int32ByteSize
	recordLength := tpos + file.Int32ByteSize

	rawLogRecord := make([]byte, recordLength)
	page := file.NewPageFrom(rawLogRecord)
	page.SetInt(0, types.Int(COMMIT))
	page.SetInt(tpos, types.Int(transactionNumber))

	return logManager.Append(page.Data)
}
