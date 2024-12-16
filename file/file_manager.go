package file

import (
	"fmt"
	"os"
	"path/filepath"
)

const fileFlag = os.O_RDWR | os.O_CREATE | os.O_SYNC

type FileRequest interface {
	// 操作の対象となるファイル名を返す
	getFileName(fm *FileManager) string

	// ファイルを開く. ファイルが存在しない場合は新規作成する
	openFile(fm *FileManager) (*os.File, error)

	// ファイルに対してのリクエストを処理する
	resolve(f *os.File, fm *FileManager)

	// エラーをリクエスト元に返す
	handleError(error)
}

type ReadFileRequest struct {
	blockID   *BlockID
	page      *Page
	errorChan chan error
}

func (rfr *ReadFileRequest) getFileName(fm *FileManager) string {
	return filepath.Join(fm.dbDirectoryPath, rfr.blockID.Filename)
}

func (rfr *ReadFileRequest) openFile(fm *FileManager) (*os.File, error) {
	return os.OpenFile(rfr.getFileName(fm), fileFlag, 0644)
}

func (rfr *ReadFileRequest) resolve(f *os.File, fm *FileManager) {
	f.Seek(int64(rfr.blockID.Blknum*fm.BlockSize()), 0)
	_, err := f.Read(rfr.page.Data)
	rfr.handleError(err)
}

func (rfr *ReadFileRequest) handleError(err error) {
	rfr.errorChan <- err
}

type WriteFileRequest struct {
	blockID   *BlockID
	page      *Page
	errorChan chan error
}

func (wfr *WriteFileRequest) getFileName(fm *FileManager) string {
	return filepath.Join(fm.dbDirectoryPath, wfr.blockID.Filename)
}

func (wfr *WriteFileRequest) resolve(f *os.File, fm *FileManager) {
	_, err := f.Seek(int64(wfr.blockID.Blknum*fm.BlockSize()), 0)
	if err != nil {
		wfr.handleError(err)
		return
	}
	_, err = f.Write(wfr.page.Data)
	wfr.handleError(err)
}

func (wfr *WriteFileRequest) openFile(fm *FileManager) (*os.File, error) {
	return os.OpenFile(wfr.getFileName(fm), fileFlag, 0644)
}

func (wfr *WriteFileRequest) handleError(err error) {
	wfr.errorChan <- err
}

type AppendFileRequest struct {
	fileName  string
	replyChan chan *BlockID
	errorChan chan error
}

func (afr *AppendFileRequest) getFileName(fm *FileManager) string {
	return filepath.Join(fm.dbDirectoryPath, afr.fileName)
}

func (afr *AppendFileRequest) openFile(fm *FileManager) (*os.File, error) {
	return os.OpenFile(afr.getFileName(fm), fileFlag, 0644)
}

func (afr *AppendFileRequest) resolve(f *os.File, fm *FileManager) {
	fileInfo, err := f.Stat()
	if err != nil {
		afr.handleError(err)
		return
	}

	fileBlockLength := fileInfo.Size() / int64(fm.BlockSize())
	blockID := NewBlockID(filepath.Base(afr.getFileName(fm)), int(fileBlockLength))
	emptyBytes := make([]byte, fm.BlockSize())
	_, err = f.Seek(int64(blockID.Blknum*fm.BlockSize()), 0)
	if err != nil {
		afr.handleError(err)
		return
	}

	_, err = f.Write(emptyBytes)
	if err != nil {
		afr.handleError(err)
		return
	} else {
		afr.errorChan <- nil
		afr.replyChan <- blockID
		return
	}
}

func (afr *AppendFileRequest) handleError(err error) {
	afr.errorChan <- err
	afr.replyChan <- nil
}

type FileManager struct {
	dbDirectoryPath string
	files           map[string]*os.File
	requestChan     chan FileRequest
	closeChan       chan bool
	blockSize       int
}

func NewFileManager(dbDirectoryPath string, blockSize int) *FileManager {
	initDbDirectory(dbDirectoryPath)
	cleanTempFiles(dbDirectoryPath)

	manager := &FileManager{
		dbDirectoryPath: dbDirectoryPath,
		files:           make(map[string]*os.File),
		requestChan:     make(chan FileRequest),
		closeChan:       make(chan bool),
		blockSize:       blockSize,
	}

	go manager.run()
	return manager
}

func initDbDirectory(dbDirectoryPath string) {
	if _, err := os.Stat(dbDirectoryPath); os.IsNotExist(err) {
		fmt.Println("DB ディレクトリが存在しません. 新規作成します.", dbDirectoryPath)
		err := os.Mkdir(dbDirectoryPath, 0755)
		if err != nil {
			fmt.Println("DB ディレクトリの作成に失敗しました.", dbDirectoryPath, err)
			return
		}
	} else {
		fmt.Println("DB ディレクトリが存在します.", dbDirectoryPath)
	}
}

func cleanTempFiles(dbDirectoryPath string) {
	matches, err := filepath.Glob(filepath.Join(dbDirectoryPath, "temp*"))

	if err != nil {
		fmt.Println("ディレクトリの読み取りに失敗しました.", err)
		return
	}

	for _, match := range matches {
		err := os.Remove(match)
		if err != nil {
			fmt.Println("tempファイルの削除に失敗しました.", err)
		} else {
			fmt.Println("tempファイルを削除しました.", match)
		}
	}
}

func (fm *FileManager) BlockSize() int {
	return fm.blockSize
}

// ファイルの読み書きを受け付ける処理
// 別のメイン goroutine で実行される
func (fm *FileManager) run() {
	for {
		select {
		case req := <-fm.requestChan:
			file, exists := fm.files[req.getFileName(fm)]
			if !exists {
				var err error
				newFile, err := req.openFile(fm)
				if err != nil {
					req.handleError(err)
					continue
				}
				file = newFile
				fm.files[req.getFileName(fm)] = newFile
			}
			req.resolve(file, fm)
			break

		case <-fm.closeChan:
			for _, file := range fm.files {
				if err := file.Close(); err != nil {
					fmt.Println("ファイルのクローズに失敗しました.", err)
				}
			}
			close(fm.requestChan)
			return
		}
	}
}

func (fm *FileManager) Read(blockID *BlockID, page *Page) {
	req := &ReadFileRequest{
		blockID:   blockID,
		page:      page,
		errorChan: make(chan error),
	}

	fm.requestChan <- req

	if err := <-req.errorChan; err != nil {
		panic(err)
	}
}

func (fm *FileManager) Write(blockID *BlockID, page *Page) {
	req := &WriteFileRequest{
		blockID:   blockID,
		page:      page,
		errorChan: make(chan error),
	}
	fm.requestChan <- req

	if err := <-req.errorChan; err != nil {
		panic(fmt.Sprintf("書き込み処理に失敗しました. %v", err))
	}
}

func (fm *FileManager) Append(fileName string) *BlockID {
	req := &AppendFileRequest{
		fileName:  fileName,
		replyChan: make(chan *BlockID),
		errorChan: make(chan error),
	}
	fm.requestChan <- req

	if err := <-req.errorChan; err != nil {
		panic(err)
	}

	return <-req.replyChan
}

func (fm *FileManager) Close() {
	fm.closeChan <- true
}
