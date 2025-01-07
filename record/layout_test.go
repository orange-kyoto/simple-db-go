package record

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLayoutCalculateSlotOffsets(t *testing.T) {
	schema := NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 10)
	schema.AddIntField("age")

	layout := NewLayout(schema)

	assert.Equal(t, FieldOffset(4), layout.GetOffset("id"), "id 列のオフセットが 4 である. empty/inuse フラグがあるので 0 ではない.")
	assert.Equal(t, FieldOffset(8), layout.GetOffset("name"), "name 列のオフセットが 8 である")
	assert.Equal(t, FieldOffset(22), layout.GetOffset("age"), "age 列のオフセットが 22 である(nameフィールドは長さ10だが、バイト列で書き込む時に、先頭４バイトにバイト列の長さを記録しているため、合計で１４バイト分になる.)")
}
