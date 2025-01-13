package record

import (
	"fmt"
	"simple-db-go/constants"
	"simple-db-go/file"
	"simple-db-go/types"
)

// DB record の物理的な構造を表現する構造体.
// 注意：Layoutは1つのスロット（あるいはレコード）の物理的な情報を表現する.
//
// ここでは一番シンプルな実装, つまり以下の設計とする.
// - Homogeneous
// - Unspanned
// - Fixed length
//
// また、先頭バイトには empty/inuse フラグがあるとする.
type Layout struct {
	schema   *Schema
	offsets  map[types.FieldName]types.FieldOffsetInSlot
	slotSize types.SlotSize
}

// テーブルが新規作成された際のコンストラクタ.
// 単に与えられた Schema を元に Layout を計算する.
func NewLayout(schema *Schema) *Layout {
	offsets := make(map[types.FieldName]types.FieldOffsetInSlot)

	// 各スロットの先頭４バイトのフラグを考慮する.
	flagPos := constants.Int32ByteSize

	for _, fieldName := range schema.Fields() {
		offsets[fieldName] = types.FieldOffsetInSlot(flagPos)
		// schema 自身から生成したフィールドの値を取得しているのでエラーは発生し得ない. 単に panic する.
		length, err := getLengthInBytes(schema, fieldName)
		if err != nil {
			panic(fmt.Sprintf("NewLayout で予期せぬエラーが発生しました. schema=%+v, fieldName=%+v, err=%+v", schema, fieldName, err))
		}
		flagPos += length
	}

	return &Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: types.SlotSize(flagPos),
	}
}

// 既存テーブルに対してのコンストラクタ.
// 既に計算された値をベースに Layout を計算する.
func NewLayoutWith(schema *Schema, offsets map[types.FieldName]types.FieldOffsetInSlot, slotSize types.SlotSize) *Layout {
	return &Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: slotSize,
	}
}

func (l *Layout) GetSchema() *Schema {
	return l.schema
}

func (l *Layout) GetOffset(fieldName types.FieldName) (types.FieldOffsetInSlot, error) {
	offset, exists := l.offsets[fieldName]
	if !exists {
		return 0, &UnknownFieldError{l.schema, fieldName}
	}
	return offset, nil
}

func (l *Layout) GetSlotSize() types.SlotSize {
	return l.slotSize
}

func getLengthInBytes(schema *Schema, fieldName types.FieldName) (types.Int, error) {
	fieldType, err := schema.FieldType(fieldName)
	if err != nil {
		return 0, err
	}

	if fieldType == constants.INTEGER {
		return constants.Int32ByteSize, nil
	}

	// fieldType == VARCHAR
	length, err := schema.Length(fieldName)
	if err != nil {
		return 0, err
	}
	return file.MaxLength(types.Int(length)), nil
}
