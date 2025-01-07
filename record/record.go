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

// DBレコードのフィールド名
type FieldName string

// DBレコードのフィールドの型
type FieldType types.Int

// DBレコードのフィールドの長さ. 文字列フィールドの場合、これは最大文字数であり、バイトサイズではない.
// 整数フィールドの場合は 0 とし、この値は使わない（固定長のため）.
type FieldLength types.Int

// 各スロット内における、フィールドのオフセット
// 前にあるフィールドの長さの合計＋フラグの長さ(4bytes)
type FieldOffsetInSlot types.Int

// 各ページ内における、フィールドのオフセット
// field_offset_in_page = slot_offset + field_offset_in_slot
type FieldOffsetInPage types.Int

// jdbc の値に合わせている.
// https://docs.oracle.com/javase/jp/8/docs/api/java/sql/Types.html
const (
	INTEGER FieldType = 4
	VARCHAR FieldType = 12
)

const (
	// 整数フィールドの場合は 0 とし、この値は使わない（固定長のため）.
	INTEGER_FIELD_LENGTH FieldLength = 0
)

type fieldInfo struct {
	fieldType FieldType
	length    FieldLength
}

func newFieldInfo(fieldType FieldType, length FieldLength) fieldInfo {
	return fieldInfo{fieldType: fieldType, length: length}
}

func calcFieldOffsetInPage(slotOffset SlotOffset, fieldOffset FieldOffsetInSlot) FieldOffsetInPage {
	return FieldOffsetInPage(types.Int(slotOffset) + types.Int(fieldOffset))
}

func slotExists(slotNumber SlotNumber) bool {
	return slotNumber > NULL_SLOT_NUMBER
}
