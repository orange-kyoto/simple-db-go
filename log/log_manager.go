package log

import (
	"fmt"
	"simple-db-go/file"
	"simple-db-go/types"
	"sync"
)

// Log Sequence Number
// 注意：1から始まる
type LSN types.Int

// Log Record
type RawLogRecord []byte

// Log Record 追加のリクエスト
type AppendRequest struct {
	record    []byte
	replyChan chan LSN
	errorChan chan error
}

var (
	logManagerInstance *LogManager
	logManagerOnce     sync.Once
)

type LogManager struct {
	fileManager *file.FileManager
	logFileName string

	// ログマネージャがメモリ上に保持する Page
	logPage *file.Page

	// 現在 Log Page に読み込んでいる BlockID
	currentBlockID *file.BlockID

	// 最新の LSN (ディスクには書き込まれていないかもしれないが、少なくともLog Page上にはある)
	latestLSN LSN

	// 最後にディスクに書き込まれた LSN
	lastSavedLSN LSN

	requestChan chan AppendRequest
	closeChan   chan bool
}

func NewLogManager(fm *file.FileManager, logFileName string) *LogManager {
	logManagerOnce.Do(func() {
		b := make([]byte, fm.BlockSize())
		logPage := file.NewPageFrom(b)
		logSize := fm.GetBlockLength(logFileName)

		logManagerInstance = &LogManager{
			fileManager:    fm,
			logFileName:    logFileName,
			currentBlockID: nil,
			logPage:        logPage,
			latestLSN:      0,
			lastSavedLSN:   0,
			requestChan:    make(chan AppendRequest),
			closeChan:      make(chan bool),
		}

		if logSize == 0 {
			logManagerInstance.currentBlockID = logManagerInstance.appendNewBlock()
		} else {
			// ログファイルの末尾のブロックを読み込む
			logManagerInstance.currentBlockID = file.NewBlockID(logFileName, logSize-1)
			fm.Read(logManagerInstance.currentBlockID, logPage)
		}

		// 排他制御のための管理用 Goroutine
		go logManagerInstance.run()
	})

	return logManagerInstance
}

func (lm *LogManager) run() {
	for {
		select {
		case req := <-lm.requestChan:
			lsn := lm.append(req.record)
			req.errorChan <- nil
			req.replyChan <- lsn
			break
		case <-lm.closeChan:
			lm.flush()
			lm.fileManager.Close()
			close(lm.requestChan)
			return
		}
	}
}

func (lm *LogManager) Flush(lsn LSN) {
	if lsn >= lm.lastSavedLSN {
		lm.flush()
	}
}

func (lm *LogManager) Append(logRecord RawLogRecord) LSN {
	req := &AppendRequest{
		record:    logRecord,
		replyChan: make(chan LSN),
		errorChan: make(chan error),
	}

	lm.requestChan <- *req

	if err := <-req.errorChan; err != nil {
		panic(fmt.Sprintf("ログの追加に失敗しました. %v", err))
	}

	return <-req.replyChan
}

// Log Page に新しいレコードを追加する.
// 注意1: log record は右から左に追加していく. (iteratorが最も新しいログから読み取れるようにしているっぽい.じゃあGoではそれに倣う必要はないかも？)
// 注意2: Log Page の先頭4バイト(boundary)は、直近で追加されたレコードのオフセット.
// 注意3: LSN という連番を管理する都合上、排他制御が必要. 管理用の Goroutine で実行する.
func (lm *LogManager) append(logRecord RawLogRecord) LSN {
	var boundary types.Int
	boundary = lm.logPage.GetInt(0)
	recordSize := types.Int(len(logRecord))
	bytesNeeded := recordSize + file.Int32ByteSize

	// 新しく必要になるバイト数(bytesNeeded) = 4 + len(logRecord)
	// 今 Log Page で空いているバイト数 = boundary - 4
	// logRecordがそのままPageに収まる条件: boundary - 4 >= bytesNeeded
	if boundary-file.Int32ByteSize < bytesNeeded {
		// Log Page に収まらない場合の処理
		lm.flush()                              // ディスクに書き込んで、
		lm.currentBlockID = lm.appendNewBlock() // 新しいブロックで Log Page を更新する.
		boundary = lm.logPage.GetInt(0)
	}

	recordPosition := boundary - bytesNeeded
	lm.logPage.SetBytes(recordPosition, logRecord)
	lm.logPage.SetInt(0, recordPosition) // This is the new boundary.
	lm.latestLSN = lm.latestLSN + 1
	return lm.latestLSN
}

// ログファイルの末尾に新しくブロックを１つ追加する
// その際にブロックサイズを Log Page の先頭に付与する(boundary)
func (lm *LogManager) appendNewBlock() *file.BlockID {
	appendedBlockID := lm.fileManager.Append(lm.logFileName)
	boundary := lm.fileManager.BlockSize()
	lm.logPage.SetInt(0, boundary)
	lm.fileManager.Write(appendedBlockID, lm.logPage)
	return appendedBlockID
}

// Log Page をディスクに書き込む
// 注：ログページの内容はそのままにしている
func (lm *LogManager) flush() {
	lm.fileManager.Write(lm.currentBlockID, lm.logPage)
	lm.lastSavedLSN = lm.latestLSN
}

func (lm *LogManager) Close() {
	lm.closeChan <- true
}

// Log Iterator に相当する処理を関数で実装する
func (lm *LogManager) StreamLogs() <-chan RawLogRecord {
	// ディスクに書き込まれていないログレコードを先に書き込んでおく.
	// 以降の処理では基本的にディスクから読み込むことになる.
	lm.flush()

	logChan := make(chan RawLogRecord)
	startBlockID := lm.currentBlockID

	// 初期化
	blockID := startBlockID
	page := file.NewPageFrom(make([]byte, lm.fileManager.BlockSize()))

	var currentPosition types.Int

	// 指定したブロックに移動する. boundary, currentPosition は最新のレコードの位置を示す.
	// ログレコードはブロック内で右から左に書き込まれることに注意.
	moveToBlock := func(destBlockID *file.BlockID) {
		lm.fileManager.Read(destBlockID, page)
		currentPosition = page.GetInt(0)
	}

	moveToBlock(startBlockID)

	go func() {
		defer close(logChan)

		hasNext := func() bool {
			return currentPosition < lm.fileManager.BlockSize() || blockID.Blknum > 0
		}

		for {
			if !hasNext() {
				// もう次のログがなければ処理を終了する
				break
			}
			fmt.Printf("blockID: %+v\n", blockID)
			fmt.Printf("currentPosition: %d\n", currentPosition)

			// このブロックにはログがないので、次のブロックに移動する
			// 最新のログから辿れるようにするので、ログファイルないの後ろのブロックから読み込むイメージ
			if currentPosition == lm.fileManager.BlockSize() {
				fmt.Printf("Moving to previous block...\n")
				blockID = file.NewBlockID(blockID.Filename, blockID.Blknum-1)
				moveToBlock(blockID)
				fmt.Printf("After moving to previous block. currentPosition=%d\n", currentPosition)
				// ちゃんとここで更新されているっぽい。
			}

			// 現在位置のログレコードを読み込む
			logRecord := page.GetBytes(currentPosition)
			fmt.Printf("--- logRecord: %+v (%s), address: %p ---\n", logRecord, string(logRecord), &logRecord)
			// 現在位置を更新する
			currentPosition += file.Int32ByteSize + types.Int(len(logRecord))
			fmt.Printf("new currentPosition: %d\n", currentPosition)

			logChan <- logRecord
		}
	}()

	return logChan
}

func (lm *LogManager) GetLSN() (latestLSN LSN, lastSavedLSN LSN) {
	return lm.latestLSN, lm.lastSavedLSN
}

func (lm *LogManager) GetLogPage() *file.Page {
	content := make([]byte, lm.fileManager.BlockSize())
	copy(content, lm.logPage.Data)
	return file.NewPageFrom(content)
}
