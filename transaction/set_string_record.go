package transaction

import (
	"fmt"
	"simple-db-go/constants"
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/types"
	"simple-db-go/util"
)

type SetStringRecord struct {
	transactionNumber types.TransactionNumber
	offset            types.Int
	// ログレコードに記録された、その操作における変更前の値
	oldValue string
	blockID  file.BlockID
}

/*
# SETSTRING レコードの構造

```example
<SETSTRING, 2, junk, 44, 20, hello, ciao>
```

* 1つ目：SETSTRING
* 2つ目：トランザクション番号
* 3つ目：書き込む対象のファイル名
* 4つ目：書き込む対象のブロック番号
* 5つ目：書き込む対象のオフセット
* 6つ目：書き込む前の古い値
* 7つ目：書き込む新しい値

おそらく、今回の実装では undo-only をやるため、7つ目の新しい値は使わない.
*/
func NewSetStringRecord(page *file.Page) *SetStringRecord {
	tpos := constants.Int32ByteSize
	txNum := types.TransactionNumber(page.GetInt(tpos))

	fpos := tpos + constants.Int32ByteSize
	filename := page.GetString(fpos)

	bpos := fpos + file.MaxLength(util.Len(filename))
	blockNumber := types.BlockNumber(page.GetInt(bpos))
	blockID := file.NewBlockID(filename, blockNumber)

	opos := bpos + constants.Int32ByteSize
	offset := page.GetInt(opos)

	vpos := opos + constants.Int32ByteSize
	oldValue := page.GetString(vpos)

	return &SetStringRecord{
		transactionNumber: txNum,
		offset:            offset,
		oldValue:          oldValue,
		blockID:           blockID,
	}
}

func (ssr *SetStringRecord) GetOperation() RecordOperator {
	return SETSTRING
}

func (ssr *SetStringRecord) GetTransactionNumber() types.TransactionNumber {
	return ssr.transactionNumber
}

func (ssr *SetStringRecord) Undo(t *Transaction) {
	t.Pin(ssr.blockID)
	// 注意：Undo のログは残さない！
	t.SetString(ssr.blockID, ssr.offset, ssr.oldValue, false)
	t.Unpin(ssr.blockID)
}

func (ssr *SetStringRecord) ToString() string {
	return fmt.Sprintf(
		"<SETSTRING %d %s %d %s>",
		ssr.transactionNumber,
		ssr.blockID.ToString(),
		ssr.offset,
		ssr.oldValue,
	)
}

func WriteSetStringRecord(
	logManager *log.LogManager,
	transactionNumber types.TransactionNumber,
	blockID file.BlockID,
	offset types.Int,
	oldValue string,
) log.LSN {
	tpos := constants.Int32ByteSize
	fpos := tpos + constants.Int32ByteSize
	bpos := fpos + file.MaxLength(util.Len(blockID.Filename))
	opos := bpos + constants.Int32ByteSize
	vpos := opos + constants.Int32ByteSize
	recordLength := vpos + file.MaxLength(util.Len(oldValue))

	rawLogRecord := make([]byte, recordLength)
	page := file.NewPageFrom(rawLogRecord)
	page.SetInt(0, types.Int(SETSTRING))
	page.SetInt(tpos, types.Int(transactionNumber))
	page.SetString(fpos, blockID.Filename)
	page.SetInt(bpos, types.Int(blockID.BlockNumber))
	page.SetInt(opos, offset)
	page.SetString(vpos, oldValue)

	return logManager.Append(page.Data)
}
