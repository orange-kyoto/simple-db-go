package file

import "simple-db-go/types"

// 指定した個数分の BlockID を用意する.
// BlockNumber は 0 から始まる連番.
func PrepareBlockIDs(num types.Int, fileName string) []BlockID {
	result := make([]BlockID, num)

	for i := types.Int(0); i < num; i++ {
		result[i] = NewBlockID(fileName, types.BlockNumber(i))
	}

	return result
}

// 指定した個数分の Page を用意する.
func PreparePages(num types.Int, blockSize types.Int) []*Page {
	result := make([]*Page, num)

	for i := types.Int(0); i < num; i++ {
		result[i] = NewPage(blockSize)
	}

	return result
}
