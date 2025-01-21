package record

import "simple-db-go/types"

// スロット番号. 0 から始まる.
type SlotNumber types.Int

// スロットが存在しないことを示す特別なスロット番号.
const NULL_SLOT_NUMBER SlotNumber = -1

// レコードページ内における、スロットのオフセット
// slot_offset = slot_number * slot_size
type SlotOffset types.Int

// スロットのフラグ. empty or inuse.
type SlotFlag types.Int

const (
	SLOT_EMPTY SlotFlag = 0
	SLOT_INUSE SlotFlag = 1
)

// 各ページ内における、フィールドのオフセット
// field_offset_in_page = slot_offset + field_offset_in_slot
type FieldOffsetInPage types.Int

const (
	// 整数フィールドの場合は 0 とし、この値は使わない（固定長のため）.
	INTEGER_FIELD_LENGTH types.FieldLength = 0
)

type fieldInfo struct {
	fieldType types.FieldType
	length    types.FieldLength
}

func newFieldInfo(fieldType types.FieldType, length types.FieldLength) fieldInfo {
	return fieldInfo{fieldType: fieldType, length: length}
}

func calcFieldOffsetInPage(slotOffset SlotOffset, fieldOffset types.FieldOffsetInSlot) FieldOffsetInPage {
	return FieldOffsetInPage(types.Int(slotOffset) + types.Int(fieldOffset))
}

func SlotExists(slotNumber SlotNumber) bool {
	return slotNumber > NULL_SLOT_NUMBER
}
