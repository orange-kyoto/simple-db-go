package transaction

import (
	"simple-db-go/buffer"
	"simple-db-go/log"
	"simple-db-go/types"
)

// # リカバリマネージャ
//
// - ここでは undo のみ実装する。redo は実装しない。
type RecoveryManager struct {
	logManager    *log.LogManager
	bufferManager *buffer.BufferManager
	transaction   *Transaction
	// TODO：transaction 内で持っているべきでは？後で見直す。
	transactionNumber types.TransactionNumber
}

func NewRecoveryManager(transaction *Transaction, transactionNumber types.TransactionNumber, logManager *log.LogManager, bufferManager *buffer.BufferManager) *RecoveryManager {
	WriteStartRecord(logManager, transactionNumber)
	return &RecoveryManager{
		logManager:        logManager,
		bufferManager:     bufferManager,
		transaction:       transaction,
		transactionNumber: transactionNumber,
	}
}

// 先に該当トランザクションに該当するログレコードや buffer pool をディスクに書き込む。
// その後、COMMIT レコードを書き込む。
func (rm *RecoveryManager) Commit() {
	rm.bufferManager.FlushAll(rm.transactionNumber)
	lsn := WriteCommitRecord(rm.logManager, rm.transactionNumber)
	rm.logManager.Flush(lsn)
}

func (rm *RecoveryManager) Rollback() {
	// doRollback では、トランザクションが変更した内容を元に戻す変更をバッファーに書き込む.
	// 仮にそのトランザクションの変更がバッファーに残っていた場合、doRollback によってまさに巻き戻し処理が発生する.
	// 最終的に、bufferManager.FlushAll でバッファーの内容をディスクに書き込む.
	// 結果的に、当該トランザクションでの変更が巻き戻される.
	rm.doRollback()
	rm.bufferManager.FlushAll(rm.transactionNumber)
	lsn := WriteRollbackRecord(rm.logManager, rm.transactionNumber)
	rm.logManager.Flush(lsn) // ROLLBACK レコードを書き込んでいる
}

func (rm *RecoveryManager) Recover() {
	rm.doRecover()
	rm.bufferManager.FlushAll(rm.transactionNumber)
	lsn := WriteCheckpointRecord(rm.logManager)
	rm.logManager.Flush(lsn)
}

// 古い値を buffer から読み出し、更新するためのログレコードをログに記録する
// undoするための情報になるっぽい。
func (rm *RecoveryManager) SetInt(buffer *buffer.Buffer, offset types.Int, newVal types.Int) log.LSN {
	oldValue := buffer.GetContents().GetInt(offset)
	blockID := buffer.GetBlockID()
	return WriteSetIntRecord(rm.logManager, rm.transactionNumber, blockID, offset, oldValue)
}

// 古い値を buffer から読み出し、更新するためのログレコードをログに記録する
// undoするための情報になるっぽい。
func (rm *RecoveryManager) SetString(buffer *buffer.Buffer, offset types.Int, newVal string) log.LSN {
	oldValue := buffer.GetContents().GetString(offset)
	blockID := buffer.GetBlockID()
	return WriteSetStringRecord(rm.logManager, rm.transactionNumber, blockID, offset, oldValue)
}

// currentBlockID からログを後ろから順に読み進めていく.
// 対象にしている TransactionNumber に該当するレコードであれば何かしら処理をする.
// START レコード: 処理を止める
// その他のレコード：undo を実行する
//
// 注意：doRecover とは違い、こちらは RecoveryManager が管理する TransactionNumber のみを対象としている.
func (rm *RecoveryManager) doRollback() {
	rawLogRecordChan := rm.logManager.StreamLogs()

	for rawLogRecord := range rawLogRecordChan {
		logRecord := CreateLogRecord(rawLogRecord)
		if logRecord.GetTransactionNumber() == rm.transactionNumber {
			if logRecord.GetOperation() == START {
				return
			} else {
				// 実態としては、Undo のための変更を Buffer Manager が Buffer に書き込んでいる.(Transactionが隠蔽している)
				logRecord.Undo(rm.transaction)
			}
		}
	}
}

// currentBlockID からログを後ろから順に読み進めていく.
// 対象にしている TransactionNumber に該当するレコードであれば何かしら処理をする.
// CHECKPOINT レコード: 処理を止める. それ以前のログはすでに処理されているとみなす.
// COMMIT, ROLLBACK レコード: そのトランザクションが終了したことを示すので、そのトランザクションに関する処理は行わない. 処理済みのトランザクション番号として記録しておく.
// それ以外のレコード: まだ COMMIT または ROLLBACK されていないトランザクションのレコードなので、undo を実行する.
//
// 注意：doRollback とは違い、こちらはコミットされていない全てのトランザクションが対象.
func (rm *RecoveryManager) doRecover() {
	finishedTransactionNumbers := make(map[types.TransactionNumber]bool)
	rawLogRecordChan := rm.logManager.StreamLogs()

	for rawLogRecord := range rawLogRecordChan {
		logRecord := CreateLogRecord(rawLogRecord)

		if logRecord.GetOperation() == CHECKPOINT {
			return
		}

		if logRecord.GetOperation() == COMMIT || logRecord.GetOperation() == ROLLBACK {
			finishedTransactionNumbers[logRecord.GetTransactionNumber()] = true
		} else if _, exists := finishedTransactionNumbers[logRecord.GetTransactionNumber()]; !exists {
			// 注意：他のトランザクションの変更も Undo する可能性がある.
			//      つまり、logRecord に記録されているトランザクション番号と、引数に渡している rm.transaction は別の番号である可能性もあることに注意.
			// 注意：recovery の処理はログを残さない. Buffer に巻き戻しの変更を加えてディスクに書き込むだけ. なので上の動作で問題ない.
			logRecord.Undo(rm.transaction)
		}
	}
}
