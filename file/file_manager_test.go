package file

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

const (
	testDir   = "./testdb"
	blockSize = 32
)

func cleanTestDir() {
	os.RemoveAll(testDir)
}

func createTempFiles() {
	os.Mkdir(testDir, 0755)
	os.Create(filepath.Join(testDir, "tempfile1"))
	os.Create(filepath.Join(testDir, "tempfile2"))
}

func TestDbDirectoryExists(t *testing.T) {
	cleanTestDir()
	defer cleanTestDir()

	if _, err := os.Stat(testDir); os.IsExist(err) {
		t.Fatalf("Expected %s to not exist, but it does", testDir)
	}

	fm := NewFileManager(testDir, blockSize)
	defer fm.Close()

	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		t.Fatalf("Expected %s to exist, but it does not", testDir)
	}
}

func TestTempFilesCleaned(t *testing.T) {
	cleanTestDir()
	defer cleanTestDir()

	createTempFiles()

	matches, _ := filepath.Glob(filepath.Join(testDir, "temp*"))
	if len(matches) != 2 {
		t.Fatalf("Expected temp files to exist, but they do not, matches=%d", len(matches))
	}

	fm := NewFileManager(testDir, blockSize)
	defer fm.Close()

	matches, _ = filepath.Glob(filepath.Join(testDir, "temp*"))
	if len(matches) != 0 {
		t.Fatalf("Expected temp files to be deleted, but they were not, matches=%d", len(matches))
	}
}

func TestReadWrite(t *testing.T) {
	cleanTestDir()
	defer cleanTestDir()

	fm := NewFileManager(testDir, blockSize)
	defer fm.Close()

	// 整数値の読み書きの検証
	block1 := NewBlockID("testfile", 0)
	page1 := NewPage(blockSize)
	page1.SetInt(4, 12345)
	fm.Write(block1, page1)
	page2 := NewPage(blockSize)
	fm.Read(block1, page2)
	value1 := page2.GetInt(4)
	if value1 != 12345 {
		t.Errorf("Expected %d, got %d", 12345, value1)
	}

	// バイト列の読み書きの検証
	block2 := NewBlockID("testfile", 1)
	page3 := NewPage(blockSize)
	page3.SetBytes(0, []byte("test"))
	fm.Write(block2, page3)
	page4 := NewPage(blockSize)
	fm.Read(block2, page4)
	value2 := page4.GetBytes(0)
	if !bytes.Equal(value2, []byte("test")) {
		t.Errorf("Expected 'test', got '%s'", value2)
	}
	// もう一度ブロック0を読み込んで、壊れていないか確認
	page5 := NewPage(blockSize)
	fm.Read(block1, page5)
	value3 := page5.GetInt(4)
	if value3 != 12345 {
		t.Errorf("Expected %d, got %d", 12345, value3)
	}

	// 文字列の読み書きの検証
	block3 := NewBlockID("testfile", 2)
	page6 := NewPage(blockSize)
	page6.SetString(0, "hello world!")
	fm.Write(block3, page6)
	page7 := NewPage(blockSize)
	fm.Read(block3, page7)
	value4 := page7.GetString(0)
	if value4 != "hello world!" {
		t.Errorf("Expected 'hello world!', got '%s'", value4)
	}
}

func TestAppend(t *testing.T) {
	cleanTestDir()
	defer cleanTestDir()

	fm := NewFileManager(testDir, blockSize)
	defer fm.Close()

	// ブロック１つだけ書き込んでおく.
	block1 := NewBlockID("testfile", 0)
	page1 := NewPage(blockSize)
	page1.SetString(4, "hello world!")
	fm.Write(block1, page1)

	// ファイルを拡張する
	appendedBlockID := fm.Append("testfile")

	// 新しく拡張されたブロックは空のバイト列が書き込まれている.
	page2 := NewPage(blockSize)
	fm.Read(appendedBlockID, page2)
	value := page2.GetBytes(0)

	if bytes.Equal(value, make([]byte, blockSize)) {
		t.Errorf("Expected empty block, got %v", value)
	}
}
