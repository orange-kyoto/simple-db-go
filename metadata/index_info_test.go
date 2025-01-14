package metadata

import (
	"simple-db-go/constants"
	"simple-db-go/record"
	"simple-db-go/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexInfoNewInedxInfo(t *testing.T) {
	transaction := newTransactionForTest(t, indexInfoTestName)
	defer transaction.Rollback()

	// テスト用のテーブルを想定.
	testTableSchema := record.NewSchema()
	testTableSchema.AddIntField("id")
	testTableSchema.AddStringField("name", 10)
	testTableSchema.AddIntField("age")
	testTableSchema.AddStringField("address", 20)
	testStatInfo := NewStatInfo(1_000, 1_000_000)

	t.Run("整数型のフィールドにインデックスを作成した場合.", func(t *testing.T) {
		indexInfo, err := NewIndexInfo("test_index", "age", testTableSchema, transaction, testStatInfo)
		assert.NoError(t, err, "インデックスの作成に失敗してはいけない.")

		actualIndexLayout := indexInfo.indexLayout
		actualIndexSchema := actualIndexLayout.GetSchema()

		t.Run("インデックスのスキーマが期待した構造になっていること.", func(t *testing.T) {
			// フィールドの有無.
			assert.True(t, actualIndexSchema.HasField("block"), "インデックスのスキーマは block フィールドを持っているべき.")
			assert.True(t, actualIndexSchema.HasField("id"), "インデックスのスキーマは id フィールドを持っているべき.")
			assert.True(t, actualIndexSchema.HasField("data_val"), "インデックスのスキーマは data_val フィールドを持っているべき.")
			assert.Len(t, actualIndexSchema.Fields(), 3, "インデックスのスキーマは 3 つのフィールドを持っているべき.")

			// フィールドの型.
			blockFieldType, err := actualIndexSchema.FieldType("block")
			if assert.NoError(t, err) {
				assert.Equal(t, constants.INTEGER, blockFieldType, "block フィールドは INTEGER 型であるべき.")
			}

			idFieldType, err := actualIndexSchema.FieldType("id")
			if assert.NoError(t, err) {
				assert.Equal(t, constants.INTEGER, idFieldType, "id フィールドは INTEGER 型であるべき.")
			}

			dataValFieldType, err := actualIndexSchema.FieldType("data_val")
			if assert.NoError(t, err) {
				assert.Equal(t, constants.INTEGER, dataValFieldType, "data_val フィールドは INTEGER 型であるべき.")
			}
		})

		t.Run("インデックスのレイアウトが期待した構造になっていること.", func(t *testing.T) {
			// flag: 4, 3つの整数型フィールド: 4 * 3 = 12, 合計 16 bytes
			expectedSlotSize := types.SlotSize(4 + 4 + 4 + 4)
			expectedBlockFieldOffset := types.FieldOffsetInSlot(4)
			expectedIdFieldOffset := types.FieldOffsetInSlot(8)
			expectedDataValFieldOffset := types.FieldOffsetInSlot(12)

			actualSlotSize := actualIndexLayout.GetSlotSize()
			actualBlockFieldOffset, _ := actualIndexLayout.GetOffset("block")
			actualIdFieldOffset, _ := actualIndexLayout.GetOffset("id")
			actualDataValFieldOffset, _ := actualIndexLayout.GetOffset("data_val")

			assert.Equal(t, expectedSlotSize, actualSlotSize, "インデックスのスロットサイズは 16 bytes であるべき.")
			assert.Equal(t, expectedBlockFieldOffset, actualBlockFieldOffset, "block フィールドのオフセットは 4 bytes であるべき.")
			assert.Equal(t, expectedIdFieldOffset, actualIdFieldOffset, "id フィールドのオフセットは 8 bytes であるべき.")
			assert.Equal(t, expectedDataValFieldOffset, actualDataValFieldOffset, "data_val フィールドのオフセットは 12 bytes であるべき.")
		})
	})

	t.Run("文字列型のフィールドにインデックスを作成した場合.", func(t *testing.T) {
		indexInfo, err := NewIndexInfo("test_index", "name", testTableSchema, transaction, testStatInfo)
		assert.NoError(t, err, "インデックスの作成に失敗してはいけない.")

		actualIndexLayout := indexInfo.indexLayout
		actualIndexSchema := actualIndexLayout.GetSchema()

		t.Run("インデックスのスキーマが期待した構造になっていること.", func(t *testing.T) {
			// フィールドの有無.
			assert.True(t, actualIndexSchema.HasField("block"), "インデックスのスキーマは block フィールドを持っているべき.")
			assert.True(t, actualIndexSchema.HasField("id"), "インデックスのスキーマは id フィールドを持っているべき.")
			assert.True(t, actualIndexSchema.HasField("data_val"), "インデックスのスキーマは data_val フィールドを持っているべき.")
			assert.Len(t, actualIndexSchema.Fields(), 3, "インデックスのスキーマは 3 つのフィールドを持っているべき.")

			// フィールドの型.
			blockFieldType, err := actualIndexSchema.FieldType("block")
			if assert.NoError(t, err) {
				assert.Equal(t, constants.INTEGER, blockFieldType, "block フィールドは INTEGER 型であるべき.")
			}

			idFieldType, err := actualIndexSchema.FieldType("id")
			if assert.NoError(t, err) {
				assert.Equal(t, constants.INTEGER, idFieldType, "id フィールドは INTEGER 型であるべき.")
			}

			dataValFieldType, err := actualIndexSchema.FieldType("data_val")
			if assert.NoError(t, err) {
				assert.Equal(t, constants.VARCHAR, dataValFieldType, "data_val フィールドは VARCHAR 型であるべき.")
			}
		})

		t.Run("インデックスのレイアウトが期待した構造になっていること.", func(t *testing.T) {
			// flag: 4bytes, block field: 4bytes, id field: 4bytes, data_val field: (4+10)bytes, 合計 26 bytes
			expectedSlotSize := types.SlotSize(26)
			expectedBlockFieldOffset := types.FieldOffsetInSlot(4)
			expectedIdFieldOffset := types.FieldOffsetInSlot(8)
			expectedDataValFieldOffset := types.FieldOffsetInSlot(12)

			actualSlotSize := actualIndexLayout.GetSlotSize()
			actualBlockFieldOffset, _ := actualIndexLayout.GetOffset("block")
			actualIdFieldOffset, _ := actualIndexLayout.GetOffset("id")
			actualDataValFieldOffset, _ := actualIndexLayout.GetOffset("data_val")

			assert.Equal(t, expectedSlotSize, actualSlotSize, "インデックスのスロットサイズは 16 bytes であるべき.")
			assert.Equal(t, expectedBlockFieldOffset, actualBlockFieldOffset, "block フィールドのオフセットは 4 bytes であるべき.")
			assert.Equal(t, expectedIdFieldOffset, actualIdFieldOffset, "id フィールドのオフセットは 8 bytes であるべき.")
			assert.Equal(t, expectedDataValFieldOffset, actualDataValFieldOffset, "data_val フィールドのオフセットは 12 bytes であるべき.")
		})
	})
}
