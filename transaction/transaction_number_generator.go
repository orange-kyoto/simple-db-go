package transaction

import (
	"simple-db-go/types"
	"sync"
	"sync/atomic"
)

// トランザクション番号をスレッドセーフに生成するための構造体.
type TransactionNumberGenerator struct {
	current types.TransactionNumber
}

var (
	transactionNumberGenerator     *TransactionNumberGenerator
	transactionNumberGeneratorOnce sync.Once
)

func NextTransactionNumber() types.TransactionNumber {
	transactionNumberGeneratorOnce.Do(func() {
		transactionNumberGenerator = &TransactionNumberGenerator{
			current: 0,
		}
	})

	return types.TransactionNumber(
		atomic.AddInt32((*int32)(&transactionNumberGenerator.current), 1),
	)
}
