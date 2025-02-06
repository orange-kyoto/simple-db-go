package indexing_test

import (
	"fmt"
	"simple-db-go/indexing"
	"simple-db-go/planning"
	"simple-db-go/query"
	"simple-db-go/transaction"
	"simple-db-go/types"
	"testing"

	"github.com/stretchr/testify/require"
)

func setupUserTable(t *testing.T, transaction *transaction.Transaction) {
	metadataManager := getMetadamanagerForTest(t, hashIndexTestName, transaction)

	createUserTable(metadataManager, transaction)
	createUserTableIndex(metadataManager, transaction)

	tablePlan, err := planning.NewTablePlan(transaction, "users", metadataManager)
	require.NoError(t, err)

	tableScan := tablePlan.Open().(query.UpdateScan)
	defer tableScan.Close()
	indexInfo, err := metadataManager.GetIndexInfo("users", transaction)
	require.NoError(t, err)

	indexes := make(map[types.FieldName]indexing.Index, len(indexInfo))
	for fieldName, info := range indexInfo {
		index := info.Open()
		indexes[fieldName] = index
	}

	recordsNum := types.Int(1_000)
	for i := types.Int(0); i < recordsNum; i++ {
		t.Logf("[i=%d] Inserting...", i)
		tableScan.Insert()
		tableScan.SetInt("id", i)
		tableScan.SetString("name", fmt.Sprintf("name%d", i))
		tableScan.SetInt("age", i%100)

		// インデックスにもレコードを追加していく.
		rid := tableScan.GetCurrentRecordID()
		for fieldName, index := range indexes {
			val, err := tableScan.GetValue(fieldName)
			require.NoError(t, err)
			index.Insert(val, rid)
		}
	}
}

func TestHashIndexSearch(t *testing.T) {
	transaction := newTransactionForTest(t, hashIndexTestName)
	defer transaction.Commit()

	setupUserTable(t, transaction)

	metadataManager := getMetadamanagerForTest(t, hashIndexTestName, transaction)
	tablePlan, err := planning.NewTablePlan(transaction, "users", metadataManager)
	require.NoError(t, err)
	tableScan := tablePlan.Open().(query.UpdateScan)
	defer tableScan.Close()

	indexInfo, err := metadataManager.GetIndexInfo("users", transaction)
	require.NoError(t, err)

	t.Run("idカラムでインデックスを用いて検索ができること.", func(t *testing.T) {
		idIndex := indexInfo["id"].Open()

		idIndex.BeforeFirst(query.NewIntConstant(999))
		require.True(t, idIndex.Next(), "レコードが1つだけ存在するはずなので最初のNext()はtrueを返す.")

		rid := idIndex.GetDataRecordID()
		tableScan.MoveToRecordID(rid)

		id, err := tableScan.GetInt("id")
		require.NoError(t, err)
		require.Equalf(t, types.Int(999), id, "id が 999 であるレコードが見つかるべき.")

		name, err := tableScan.GetString("name")
		require.NoError(t, err)
		require.Equalf(t, "name999", name, "name が name999 であるレコードが見つかるべき.")

		age, err := tableScan.GetInt("age")
		require.NoError(t, err)
		require.Equalf(t, types.Int(99), age, "age が 99 であるレコードが見つかるべき.")

		require.False(t, idIndex.Next(), "次のレコードが存在しないのでNext()はfalseを返す.")
	})

	t.Run("nameカラムでインデックスを用いて検索ができること.", func(t *testing.T) {
		nameIndex := indexInfo["name"].Open()

		nameIndex.BeforeFirst(query.NewStrConstant("name877"))
		require.True(t, nameIndex.Next(), "レコードが1つだけ存在するはずなので最初のNext()はtrueを返す.")

		rid := nameIndex.GetDataRecordID()
		tableScan.MoveToRecordID(rid)

		id, err := tableScan.GetInt("id")
		require.NoError(t, err)
		require.Equalf(t, types.Int(877), id, "id が 877 であるレコードが見つかるべき.")

		name, err := tableScan.GetString("name")
		require.NoError(t, err)
		require.Equalf(t, "name877", name, "name が name877 であるレコードが見つかるべき.")

		age, err := tableScan.GetInt("age")
		require.NoError(t, err)
		require.Equalf(t, types.Int(77), age, "age が 77 であるレコードが見つかるべき.")

		require.False(t, nameIndex.Next(), "次のレコードが存在しないのでNext()はfalseを返す.")
	})

	t.Run("ageカラムでインデックスを用いて検索ができること.", func(t *testing.T) {
		ageIndex := indexInfo["age"].Open()

		ageIndex.BeforeFirst(query.NewIntConstant(55))
		recordsCount := 0 // ヒットしたレコード数をカウントする.

		for ageIndex.Next() {
			recordsCount++

			rid := ageIndex.GetDataRecordID()
			tableScan.MoveToRecordID(rid)

			age, err := tableScan.GetInt("age")
			require.NoError(t, err)
			require.Equal(t, types.Int(55), age, "age が 55 であるレコードが見つかるべき.")
		}

		require.Equal(t, 10, recordsCount, "age が 55 であるレコードは100件存在するはず.")
	})
}
