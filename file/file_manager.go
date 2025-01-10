package file

import (
	"fmt"
	"os"
	"path/filepath"
	"simple-db-go/types"
)

const fileFlag = os.O_RDWR | os.O_CREATE | os.O_SYNC

type FileManager struct {
	dbDirectoryPath string
	files           map[string]*os.File
	requestChan     chan FileRequest
	closeChan       chan bool
	blockSize       types.Int
	isNew           bool
}

// NOTE: シングルトンにすることを検討したが、テストが複雑になりそうなのと、あくまで学習用のアプリケーションなので、特に複雑な管理はしない。
func NewFileManager(dbDirectoryPath string, blockSize types.Int) *FileManager {
	fm := &FileManager{
		dbDirectoryPath: dbDirectoryPath,
		files:           make(map[string]*os.File),
		requestChan:     make(chan FileRequest),
		closeChan:       make(chan bool),
		blockSize:       blockSize,
	}

	fm.initDbDirectory()
	fm.cleanTempFiles()

	go fm.run()

	return fm
}

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
	blockID   BlockID
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
	offset := types.Int(rfr.blockID.BlockNumber) * fm.BlockSize()
	f.Seek(int64(offset), 0)
	_, err := f.Read(rfr.page.Data)
	rfr.handleError(err)
}

func (rfr *ReadFileRequest) handleError(err error) {
	rfr.errorChan <- err
}

type WriteFileRequest struct {
	blockID   BlockID
	page      *Page
	errorChan chan error
}

func (wfr *WriteFileRequest) getFileName(fm *FileManager) string {
	return filepath.Join(fm.dbDirectoryPath, wfr.blockID.Filename)
}

func (wfr *WriteFileRequest) resolve(f *os.File, fm *FileManager) {
	offset := types.Int(wfr.blockID.BlockNumber) * fm.BlockSize()
	_, err := f.Seek(int64(offset), 0)
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
	replyChan chan BlockID
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
	blockID := NewBlockID(filepath.Base(afr.getFileName(fm)), types.BlockNumber(fileBlockLength))
	emptyBytes := make([]byte, fm.BlockSize())
	offset := types.Int(blockID.BlockNumber) * fm.BlockSize()
	_, err = f.Seek(int64(offset), 0)
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
	// afr.replyChan <- nil
}

type GetBlockLength struct {
	fileName  string
	replyChan chan types.Int
	errorChan chan error
}

func (gbl *GetBlockLength) getFileName(fm *FileManager) string {
	return filepath.Join(fm.dbDirectoryPath, gbl.fileName)
}

func (gbl *GetBlockLength) openFile(fm *FileManager) (*os.File, error) {
	return os.OpenFile(gbl.getFileName(fm), fileFlag, 0644)
}

func (gbl *GetBlockLength) resolve(f *os.File, fm *FileManager) {
	fileInfo, err := f.Stat()
	if err != nil {
		gbl.handleError(err)
		return
	} else {
		gbl.errorChan <- nil
		gbl.replyChan <- types.Int(fileInfo.Size() / int64(fm.BlockSize()))
		return
	}
}

func (gbl *GetBlockLength) handleError(err error) {
	gbl.errorChan <- err
	gbl.replyChan <- -1
}

func (fm *FileManager) initDbDirectory() {
	if _, err := os.Stat(fm.dbDirectoryPath); os.IsNotExist(err) {
		fm.isNew = true
		err := os.Mkdir(fm.dbDirectoryPath, 0755)
		if err != nil {
			panic(fmt.Sprintf("DB ディレクトリの作成に失敗しました. %+v", err))
		}
	} else {
		fm.isNew = false
	}
}

func (fm *FileManager) cleanTempFiles() {
	matches, err := filepath.Glob(filepath.Join(fm.dbDirectoryPath, "temp*"))

	if err != nil {
		panic(fmt.Sprintf("FileManager 起動時に temp ファイルの削除に失敗しました. %+v", err))
	}

	for _, match := range matches {
		err := os.Remove(match)
		if err != nil {
			fmt.Println("tempファイルの削除に失敗しました.", err)
		}
	}
}

func (fm *FileManager) BlockSize() types.Int {
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

func (fm *FileManager) Read(blockID BlockID, page *Page) {
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

func (fm *FileManager) Write(blockID BlockID, page *Page) {
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

// ファイルに新しくブロックを追加する。追加された分のブロックはまだ空。
func (fm *FileManager) Append(fileName string) BlockID {
	req := &AppendFileRequest{
		fileName:  fileName,
		replyChan: make(chan BlockID),
		errorChan: make(chan error),
	}
	fm.requestChan <- req

	if err := <-req.errorChan; err != nil {
		panic(err)
	}

	return <-req.replyChan
}

func (fm *FileManager) GetBlockLength(fileName string) types.Int {
	req := &GetBlockLength{
		fileName:  fileName,
		replyChan: make(chan types.Int),
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

func (fm *FileManager) IsNew() bool {
	return fm.isNew
}
