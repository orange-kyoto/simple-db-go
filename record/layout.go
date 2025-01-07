package record

import (
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
	offsets  map[FieldName]FieldOffset
	slotSize types.Int
}

// テーブルが新規作成された際のコンストラクタ.
// 単に与えられた Schema を元に Layout を計算する.
func NewLayout(schema *Schema) *Layout {
	offsets := make(map[FieldName]FieldOffset)

	// 各スロットの先頭４バイトのフラグを考慮する.
	flagPos := constants.Int32ByteSize

	for _, fieldName := range schema.Fields() {
		offsets[fieldName] = FieldOffset(flagPos)
		flagPos += getLengthInBytes(schema, fieldName)
	}

	return &Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: flagPos,
	}
}

// 既存テーブルに対してのコンストラクタ.
// 既に計算された値をベースに Layout を計算する.
func NewLayoutWith(schema *Schema, offsets map[FieldName]FieldOffset, slotSize types.Int) *Layout {
	return &Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: slotSize,
	}
}

func (l *Layout) GetSchema() *Schema {
	return l.schema
}

func (l *Layout) GetOffset(fieldName FieldName) FieldOffset {
	offset, _ := l.offsets[fieldName]
	return offset
}

func (l *Layout) GetSlotSize() types.Int {
	return l.slotSize
}

func getLengthInBytes(schema *Schema, fieldName FieldName) types.Int {
	fieldType := schema.FieldType(fieldName)

	if fieldType == INTEGER {
		return constants.Int32ByteSize
	}

	// fieldType == VARCHAR
	length := types.Int(schema.Length(fieldName))
	return file.MaxLength(length)
}
