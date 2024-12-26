package transaction

import (
	"simple-db-go/buffer"
	"simple-db-go/log"
	"simple-db-go/types"
)

// TODO: 全体的に実装は後でやる！

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
	rm.doRollback()
	// ロールバックの処理を実行した後に buffer manager の flush を行うのは大丈夫なのか？逆じゃないのか？
	// いや、Undo の実装は、Buffer Manager が Buffer に書き込む処理を行うので、この順番で問題ない。
	// 気になるのは、doRollback() を実行した時に、Buffer Pool 内にすでに書き込まれた変更があるのではないか？それらはどうなるのか？というところ。もしかしたら、StreamLogs() でうまくやっている？
	// →もしかしたら、Transaction の実装がまだないからわかっていないだけかもしれない。
	// TODO: Transaction の実装をした後に、もう一度見直してみる。
	// 			少なくとも、Undo で Buffer Pool に変更を加えるので、doRollback の後にFlushAllが1度必要なのは理解した。
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
			logRecord.Undo(rm.transaction)
		}
	}
}
