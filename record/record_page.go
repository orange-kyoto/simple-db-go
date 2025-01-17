package record

import (
	"fmt"
	"simple-db-go/constants"
	"simple-db-go/file"
	"simple-db-go/transaction"
	"simple-db-go/types"
)

// スロットの集まりを表現する構造体.
type RecordPage struct {
	transaction *transaction.Transaction
	blockID     file.BlockID
	layout      *Layout
}

func NewRecordPage(transaction *transaction.Transaction, blockID file.BlockID, layout *Layout) *RecordPage {
	return &RecordPage{
		transaction: transaction,
		blockID:     blockID,
		layout:      layout,
	}
}

func (rp *RecordPage) GetBlockID() file.BlockID {
	return rp.blockID
}

func (rp *RecordPage) GetInt(slotNumber SlotNumber, fieldName types.FieldName) (types.Int, error) {
	fieldOffset, err := rp.getFieldOffsetInPage(slotNumber, fieldName)
	if err != nil {
		return 0, err
	}
	return rp.transaction.GetInt(rp.blockID, types.Int(fieldOffset)), nil
}

func (rp *RecordPage) GetString(slotNumber SlotNumber, fieldName types.FieldName) (string, error) {
	fieldOffset, err := rp.getFieldOffsetInPage(slotNumber, fieldName)
	if err != nil {
		return "", err
	}
	return rp.transaction.GetString(rp.blockID, types.Int(fieldOffset)), nil
}

func (rp *RecordPage) SetInt(slotNumber SlotNumber, fieldName types.FieldName, value types.Int) error {
	fieldOffset, err := rp.getFieldOffsetInPage(slotNumber, fieldName)
	if err != nil {
		return err
	}
	rp.transaction.SetInt(rp.blockID, types.Int(fieldOffset), value, true)
	return nil
}

func (rp *RecordPage) SetString(slotNumber SlotNumber, fieldName types.FieldName, value string) error {
	fieldOffset, err := rp.getFieldOffsetInPage(slotNumber, fieldName)
	if err != nil {
		return err
	}
	rp.transaction.SetString(rp.blockID, types.Int(fieldOffset), value, true)
	return nil
}

// このレコードページ内の全てのスロットを初期化する.
// 整数は0、文字列は空文字列に初期化する.
func (rp *RecordPage) Format() {
	for slotNumber := SlotNumber(0); rp.isValidSlot(slotNumber); slotNumber++ {
		// フラグの初期値を EMPTY に設定する.
		slotOffset := rp.getSlotOffset(slotNumber)
		rp.transaction.SetInt(rp.blockID, types.Int(slotOffset), types.Int(SLOT_EMPTY), false)

		// 各フィールドの初期値を設定する.
		schema := rp.layout.GetSchema()
		for _, fieldName := range schema.Fields() {
			fieldOffset, err := rp.getFieldOffsetInPage(slotNumber, fieldName)
			if err != nil {
				// ここの fieldName は schema からとっているので、必ず存在するはず.
				// エラーになること自体が異常なので、単に panic する.
				panic(fmt.Sprintf("RecordPage.Format で予期せぬエラーが発生しました. record_page=%+v, err=%+v", rp, err))
			}

			fieldType, err := schema.FieldType(fieldName)
			if err != nil {
				// ここの fieldName は schema からとっているので、必ず存在するはず.
				// エラーになること自体が異常なので、単に panic する.
				panic(fmt.Sprintf("RecordPage.Format で予期せぬエラーが発生しました. record_page=%+v, err=%+v", rp, err))
			}

			if fieldType == constants.INTEGER {
				rp.transaction.SetInt(rp.blockID, types.Int(fieldOffset), types.Int(0), false)
			}

			if fieldType == constants.VARCHAR {
				rp.transaction.SetString(rp.blockID, types.Int(fieldOffset), "", false)
			}
		}
	}
}

// スロットのフラグに EMPTY をセットする.
func (rp *RecordPage) Delete(slotNumber SlotNumber) {
	rp.setSlotFlag(slotNumber, SLOT_EMPTY)
}

// 引数で指定したスロットの後ろにある、使用されている最初のスロットIDを返す.
// そのようなスロットが存在しない場合は、-1を返す.
func (rp *RecordPage) FindUsedSlotAfter(slotNumber SlotNumber) SlotNumber {
	return rp.searchAfter(slotNumber, SLOT_INUSE)
}

// 引数で指定したスロットの後ろにある、使用されていない最初のスロットIDを返す.
// もし見つかれば、そのスロットのフラグを使用中に変更する.
// そのようなスロットが存在しない場合は、-1を返す.
func (rp *RecordPage) FindEmptySlotAfter(slotNumber SlotNumber) SlotNumber {
	newSlot := rp.searchAfter(slotNumber, SLOT_EMPTY)
	if slotExists(newSlot) {
		rp.setSlotFlag(newSlot, SLOT_INUSE)
	}
	return newSlot
}

func (rp *RecordPage) setSlotFlag(slotNumber SlotNumber, slotFlag SlotFlag) {
	slotOffset := rp.getSlotOffset(slotNumber)
	rp.transaction.SetInt(rp.blockID, types.Int(slotOffset), types.Int(slotFlag), true)
}

func (rp *RecordPage) searchAfter(slotNumber SlotNumber, slotFlag SlotFlag) SlotNumber {
	for targetSlotNumber := slotNumber + 1; rp.isValidSlot(targetSlotNumber); targetSlotNumber++ {
		slotOffset := rp.getSlotOffset(targetSlotNumber)
		if rp.transaction.GetInt(rp.blockID, types.Int(slotOffset)) == types.Int(slotFlag) {
			return targetSlotNumber
		}
	}
	return NULL_SLOT_NUMBER
}

func (rp *RecordPage) isValidSlot(slotNumber SlotNumber) bool {
	return types.Int(rp.getSlotOffset(slotNumber+1)) <= rp.transaction.BlockSize()
}

func (rp *RecordPage) getSlotOffset(slotNumber SlotNumber) SlotOffset {
	return SlotOffset(types.Int(slotNumber) * types.Int(rp.layout.GetSlotSize()))
}

func (rp *RecordPage) getFieldOffsetInPage(slotNumber SlotNumber, fieldName types.FieldName) (FieldOffsetInPage, error) {
	slotOffset := rp.getSlotOffset(slotNumber)
	fieldOffsetInSlot, err := rp.layout.GetOffset(fieldName)
	if err != nil {
		return 0, err
	}
	return calcFieldOffsetInPage(slotOffset, fieldOffsetInSlot), nil
}
