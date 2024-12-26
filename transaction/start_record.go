package transaction

import (
	"fmt"
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/types"
)

type StartRecord struct {
	transactionNumber types.TransactionNumber
}

/*
# START レコードの構造

```example
<START 0>
```

* 1つ目：START
* 2つ目：トランザクション番号
*/
func NewStartRecord(page *file.Page) *StartRecord {
	tpos := file.Int32ByteSize
	txNum := types.TransactionNumber(page.GetInt(tpos))

	return &StartRecord{
		transactionNumber: txNum,
	}
}

func (sr *StartRecord) GetOperation() RecordOperator {
	return START
}

func (sr *StartRecord) GetTransactionNumber() types.TransactionNumber {
	return sr.transactionNumber
}

func (sr *StartRecord) Undo(t *Transaction) {
	// することがないので何もしない
}

func (sr *StartRecord) ToString() string {
	return fmt.Sprintf("<START %d>", sr.transactionNumber)
}

func WriteStartRecord(logManager *log.LogManager, transactionNumber types.TransactionNumber) log.LSN {
	tpos := file.Int32ByteSize
	recordLength := tpos + file.Int32ByteSize

	rawLogRecord := make([]byte, 0, recordLength)
	page := file.NewPageFrom(rawLogRecord)
	page.SetInt(0, int32(START))
	page.SetInt(tpos, int32(transactionNumber))

	return logManager.Append(page.Data)
}
