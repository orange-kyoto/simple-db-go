package record

import (
	"simple-db-go/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLayoutCalculateSlotOffsets(t *testing.T) {
	schema := NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 10)
	schema.AddIntField("age")

	layout := NewLayout(schema)

	t.Run("スロットサイズが期待した値であること.", func(t *testing.T) {
		flagSize := types.Int(4)
		idFieldSize := types.Int(4)
		nameFieldSize := types.Int(4 + 10)
		ageFieldSize := types.Int(4)
		expectedSlotSize := types.SlotSize(flagSize + idFieldSize + nameFieldSize + ageFieldSize)

		assert.Equal(t, expectedSlotSize, layout.GetSlotSize(), "スロットサイズが期待した値であること.")
	})

	t.Run("各フィールドのオフセットが期待した値になっていること.", func(t *testing.T) {
		idOffset, idOffsetErr := layout.GetOffset("id")
		nameOffset, nameOffsetErr := layout.GetOffset("name")
		ageOffset, ageOffsetErr := layout.GetOffset("age")
		if assert.NoError(t, idOffsetErr) && assert.NoError(t, nameOffsetErr) && assert.NoError(t, ageOffsetErr) {
			assert.Equal(t, types.FieldOffsetInSlot(4), idOffset, "id 列のオフセットが 4 である. empty/inuse フラグがあるので 0 ではない.")
			assert.Equal(t, types.FieldOffsetInSlot(8), nameOffset, "name 列のオフセットが 8 である")
			assert.Equal(t, types.FieldOffsetInSlot(22), ageOffset, "age 列のオフセットが 22 である(nameフィールドは長さ10だが、バイト列で書き込む時に、先頭４バイトにバイト列の長さを記録しているため、合計で１４バイト分になる.)")
		}
	})
}

func TestLayoutGetOffset(t *testing.T) {
	schema := NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 10)
	schema.AddIntField("age")

	layout := NewLayout(schema)

	t.Run("存在しないフィールド名を指定した場合、エラーが返ること.", func(t *testing.T) {
		_, err := layout.GetOffset("unknown")
		assert.Error(t, err, "存在しないフィールド名を指定した場合、エラーが返ること.")
		assert.IsType(t, &UnknownFieldError{}, err, "エラーの型が UnknownFieldInLayoutError であること.")
	})
}
