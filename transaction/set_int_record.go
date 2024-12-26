package transaction

import (
	"fmt"
	"simple-db-go/file"
	"simple-db-go/log"
	"simple-db-go/types"
	"simple-db-go/util"
)

// TODO: ここに限った話ではないが、int32 に統一したいな。それで十分だし、そういう仕様ということにしたい。

type SetIntRecord struct {
	transactionNumber types.TransactionNumber
	offset            types.Int
	// ログレコードに記録された、その操作における変更前の値
	oldValue types.Int
	blockID  *file.BlockID
}

/*
# SETINT レコードの構造

```example
<SETINT, 0, junk, 33, 8, 542, 543>
```

* 1つ目：SETINT
* 2つ目：トランザクション番号
* 3つ目：書き込む対象のファイル名
* 4つ目：書き込む対象のブロック番号
* 5つ目：書き込む対象のオフセット
* 6つ目：書き込む前の古い値
* 7つ目：書き込む新しい値
*/
func NewSetIntRecord(page *file.Page) *SetIntRecord {
	tpos := file.Int32ByteSize
	txNum := types.TransactionNumber(page.GetInt(tpos))

	fpos := tpos + file.Int32ByteSize
	filename := page.GetString(fpos)

	bpos := fpos + file.MaxLength(util.Len(filename))
	blockNumber := page.GetInt(bpos)
	blockID := file.NewBlockID(filename, blockNumber)

	opos := bpos + file.Int32ByteSize
	offset := page.GetInt(opos)

	vpos := opos + file.Int32ByteSize
	oldValue := page.GetInt(vpos)

	return &SetIntRecord{
		transactionNumber: txNum,
		offset:            offset,
		oldValue:          oldValue,
		blockID:           blockID,
	}
}

func (sir *SetIntRecord) GetOperation() RecordOperator {
	return SETINT
}

func (sir *SetIntRecord) GetTransactionNumber() types.TransactionNumber {
	return sir.transactionNumber
}

func (sir *SetIntRecord) Undo(t *Transaction) {
	t.Pin(sir.blockID)
	// 注意：Undo のログは残さない！
	t.SetInt(sir.blockID, sir.offset, sir.oldValue, false)
	t.Unpin(sir.blockID)
}

func (sir *SetIntRecord) ToString() string {
	return fmt.Sprintf(
		"<SETINT %d %s %d %d>",
		sir.transactionNumber,
		sir.blockID.ToString(),
		sir.offset,
		sir.oldValue,
	)
}

func WriteSetIntRecord(
	logManager *log.LogManager,
	txNum types.TransactionNumber,
	blockID *file.BlockID,
	offset types.Int,
	oldValue types.Int,
) log.LSN {
	tpos := file.Int32ByteSize
	fpos := tpos + file.Int32ByteSize
	bpos := fpos + file.MaxLength(util.Len(blockID.Filename))
	opos := bpos + file.Int32ByteSize
	vpos := opos + file.Int32ByteSize
	recordLength := vpos + file.Int32ByteSize // value が int32. つまり 4bytes.

	rawLogRecord := make([]byte, 0, recordLength)
	page := file.NewPageFrom(rawLogRecord)
	page.SetInt(0, types.Int(SETINT))
	page.SetInt(tpos, types.Int(txNum))
	page.SetString(fpos, blockID.Filename)
	page.SetInt(bpos, blockID.Blknum)
	page.SetInt(opos, offset)
	page.SetInt(vpos, oldValue)

	return logManager.Append(page.Data)
}
