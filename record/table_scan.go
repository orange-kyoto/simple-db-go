package record

import (
	"simple-db-go/file"
	"simple-db-go/transaction"
	"simple-db-go/types"
)

// 単一のテーブルに対してのレコードアクセスを管理する.
// ブロックの存在を意識せず、論理的なレコード操作が可能になる.
// TODO: UpdateScan なる Interface を実装すべきだが、詳細は Chapter8 で.
type TableScan struct {
	transaction       *transaction.Transaction
	layout            *Layout
	recordPage        *RecordPage
	fileName          string
	currentSlotNumber SlotNumber
}

func NewTableScan(transaction *transaction.Transaction, tableName types.TableName, layout *Layout) *TableScan {
	tableScan := &TableScan{
		transaction:       transaction,
		layout:            layout,
		fileName:          string(tableName) + ".table",
		currentSlotNumber: NULL_SLOT_NUMBER,
	}

	if transaction.Size(tableScan.fileName) == 0 {
		tableScan.moveToNewBlock()
	} else {
		tableScan.moveToBlock(0)
	}

	return tableScan
}

// RecordPage に読み込んでいたブロックIDを解放（Unpin）する.
func (ts *TableScan) Close() {
	if ts.recordPage != nil {
		blockID := ts.recordPage.GetBlockID()
		ts.transaction.Unpin(blockID)
	}
}

func (ts *TableScan) HasField(fieldName FieldName) bool {
	return ts.layout.schema.HasField(fieldName)
}

// current record を最初のレコードの直前にセットする.
func (ts *TableScan) BeforeFirst() {
	ts.moveToBlock(0)
}

// 次のレコードに移動する。レコードが存在しない場合は false を返す.
func (ts *TableScan) Next() bool {
	// 使われているスロット（＝次のレコード）があればそれで終了.
	ts.currentSlotNumber = ts.recordPage.FindUsedSlotAfter(ts.currentSlotNumber)

	// 次のレコードがない場合は、、、
	for !slotExists(ts.currentSlotNumber) {
		// 最後のブロックであれば、もう次のレコードはないので終了.
		if ts.isLastBlock() {
			return false
		}
		// 最後でなければ、次のブロックに移動してレコードを探す.
		ts.moveToNextBlock()
		ts.currentSlotNumber = ts.recordPage.FindUsedSlotAfter(ts.currentSlotNumber)
	}

	return true
}

// current record を指定された RecordID に移動する.
func (ts *TableScan) MoveToRecordID(recordID RecordID) {
	ts.moveToBlock(recordID.blockNumber)
	ts.currentSlotNumber = recordID.slotNumber
}

// current record のブロックからスタートし、新しいレコードをファイルに追加する.
// RecordPage では Insert (FindEmptySlotAfter) は失敗する可能性があるが、これは必ず成功する.
// ブロックがいっぱいの場合には、新しいブロックを割り当てるため.
func (ts *TableScan) Insert() {
	// 現在のレコードページに空きスロットがあれば、その空いているスロット番号をセットして終了.
	ts.currentSlotNumber = ts.recordPage.FindEmptySlotAfter(ts.currentSlotNumber)

	// 現在のレコードページに空きスロットがない場合、新しいブロックをなんとか用意する.
	for !slotExists(ts.currentSlotNumber) {
		if ts.isLastBlock() {
			ts.moveToNewBlock()
		} else {
			ts.moveToNextBlock()
		}
		ts.currentSlotNumber = ts.recordPage.FindEmptySlotAfter(ts.currentSlotNumber)
	}
}

func (ts *TableScan) GetInt(fieldName FieldName) types.Int {
	return ts.recordPage.GetInt(ts.currentSlotNumber, fieldName)
}

func (ts *TableScan) GetString(fieldName FieldName) string {
	return ts.recordPage.GetString(ts.currentSlotNumber, fieldName)
}

// TODO: Chapter8 で実装する.
// `Constant` は int, string の抽象化だそう.
// func (ts *TableScan) GetValue(fieldName FieldName) Constant {}

// NOTE: UpdateScan interface の要件.
func (ts *TableScan) SetInt(fieldName FieldName, val types.Int) {
	ts.recordPage.SetInt(ts.currentSlotNumber, fieldName, val)
}

// NOTE: UpdateScan interface の要件.
func (ts *TableScan) SetString(fieldName FieldName, val string) {
	ts.recordPage.SetString(ts.currentSlotNumber, fieldName, val)
}

// TODO: Chapter8 で実装する.
// `Constant` は int, string の抽象化だそう.
// func (ts *TableScan) SetValue(fieldName, value Constant)

// 現在の RecordID を返す.
func (ts *TableScan) GetCurrentRecordID() RecordID {
	blockNumber := ts.recordPage.GetBlockID().BlockNumber
	return NewRecordID(blockNumber, ts.currentSlotNumber)
}

// current record の現在のスロットを削除する（EMPTYにする）.
func (ts *TableScan) Delete() {
	if ts.currentSlotNumber != NULL_SLOT_NUMBER {
		ts.recordPage.Delete(ts.currentSlotNumber)
	}
}

// 指定したブロック番号に移動する.
// current_slot_number は`-1`にリセットする. そのブロックの最初のレコードの直前、ということになる.
func (ts *TableScan) moveToBlock(blockNumber types.BlockNumber) {
	ts.Close()
	blockID := file.NewBlockID(ts.fileName, blockNumber)

	// おそらく書籍では pin が漏れていると思われる.
	// TableScan のクライアントからはもう Block, Buffer などは完全に隠蔽したいのでここで実施する.
	// あるいは、RecordPage.SetInt などのメソッド内で実施するのが良いかもしれない.
	ts.transaction.Pin(blockID)

	ts.recordPage = NewRecordPage(ts.transaction, blockID, ts.layout)
	ts.currentSlotNumber = NULL_SLOT_NUMBER
}

func (ts *TableScan) moveToNextBlock() {
	nextBlockNumber := ts.recordPage.GetBlockID().BlockNumber + 1
	ts.moveToBlock(nextBlockNumber)
}

// 新しくブロックを追加し、そのブロックに移動する.
func (ts *TableScan) moveToNewBlock() {
	ts.Close()
	blockID := ts.transaction.Append(ts.fileName)

	// おそらく書籍では pin が漏れていると思われる.
	// TableScan のクライアントからはもう Block, Buffer などは完全に隠蔽したいのでここで実施する.
	// あるいは、RecordPage.SetInt などのメソッド内で実施するのが良いかもしれない.
	ts.transaction.Pin(blockID)

	ts.recordPage = NewRecordPage(ts.transaction, blockID, ts.layout)
	// 新しく追加されたまっさらなブロックに追加していきたいので、初期化する.
	ts.recordPage.Format()
	ts.currentSlotNumber = NULL_SLOT_NUMBER
}

func (ts *TableScan) isLastBlock() bool {
	currentBlockNumber := ts.recordPage.GetBlockID().BlockNumber

	fileBlockSize := ts.transaction.Size(ts.fileName)
	lastBlockNumber := types.BlockNumber(fileBlockSize - 1)

	return currentBlockNumber == lastBlockNumber
}
