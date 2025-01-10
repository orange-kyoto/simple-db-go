package file

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestFileManagerInitialization(t *testing.T) {
	getFileManagerForTest(t)

	t.Run("DB ディレクトリが存在すること.", func(t *testing.T) {
		if _, err := os.Stat(fileManagerTestName); os.IsNotExist(err) {
			t.Fatalf("Expected %s to exist, but it does not", fileManagerTestName)
		}
	})

	t.Run("temp ファイルが存在しないこと.", func(t *testing.T) {
		matches, _ := filepath.Glob(filepath.Join(fileManagerTestName, "temp*"))
		if len(matches) != 0 {
			t.Fatalf("Expected temp files to be deleted, but they were not, matches=%d", len(matches))
		}
	})
}

func TestReadWrite(t *testing.T) {
	fileManager := getFileManagerForTest(t)

	// 整数値の読み書きの検証
	block1 := NewBlockID("testfile", 0)
	page1 := NewPage(blockSize)
	page1.SetInt(4, 12345)
	fileManager.Write(block1, page1)
	page2 := NewPage(blockSize)
	fileManager.Read(block1, page2)
	value1 := page2.GetInt(4)
	if value1 != 12345 {
		t.Errorf("Expected %d, got %d", 12345, value1)
	}

	// バイト列の読み書きの検証
	block2 := NewBlockID("testfile", 1)
	page3 := NewPage(blockSize)
	page3.SetBytes(0, []byte("test"))
	fileManager.Write(block2, page3)
	page4 := NewPage(blockSize)
	fileManager.Read(block2, page4)
	value2 := page4.GetBytes(0)
	if !bytes.Equal(value2, []byte("test")) {
		t.Errorf("Expected 'test', got '%s'", value2)
	}
	// もう一度ブロック0を読み込んで、壊れていないか確認
	page5 := NewPage(blockSize)
	fileManager.Read(block1, page5)
	value3 := page5.GetInt(4)
	if value3 != 12345 {
		t.Errorf("Expected %d, got %d", 12345, value3)
	}

	// 文字列の読み書きの検証
	block3 := NewBlockID("testfile", 2)
	page6 := NewPage(blockSize)
	page6.SetString(0, "hello world!")
	fileManager.Write(block3, page6)
	page7 := NewPage(blockSize)
	fileManager.Read(block3, page7)
	value4 := page7.GetString(0)
	if value4 != "hello world!" {
		t.Errorf("Expected 'hello world!', got '%s'", value4)
	}
}

func TestAppend(t *testing.T) {
	fm := getFileManagerForTest(t)

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
