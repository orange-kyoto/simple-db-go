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
		expectedSlotSize := flagSize + idFieldSize + nameFieldSize + ageFieldSize

		assert.Equal(t, expectedSlotSize, layout.GetSlotSize(), "スロットサイズが期待した値であること.")
	})

	t.Run("各フィールドのオフセットが期待した値になっていること.", func(t *testing.T) {
		assert.Equal(t, types.FieldOffsetInSlot(4), layout.GetOffset("id"), "id 列のオフセットが 4 である. empty/inuse フラグがあるので 0 ではない.")
		assert.Equal(t, types.FieldOffsetInSlot(8), layout.GetOffset("name"), "name 列のオフセットが 8 である")
		assert.Equal(t, types.FieldOffsetInSlot(22), layout.GetOffset("age"), "age 列のオフセットが 22 である(nameフィールドは長さ10だが、バイト列で書き込む時に、先頭４バイトにバイト列の長さを記録しているため、合計で１４バイト分になる.)")
	})

}
