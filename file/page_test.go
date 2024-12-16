package file

import "testing"

func TestPage(t *testing.T) {
	page := NewPage(4096)

	page.SetInt(4, 12345)

	value := page.GetInt(4)
	if value != 12345 {
		t.Errorf("Expected %d, got %d", 12345, value)
	}

	page.SetBytes(8, []byte("test"))

	// page に書き込まれたバイト列に先立って、バイト列の大きさが書き込まれていること.
	value = page.GetInt(8)
	if int(value) != len([]byte("test")) {
		t.Errorf("Expected %d, got %d", len([]byte("test")), value)
	}

	// page に書き込まれたバイト列の内容が正しいこと.
	bytes := page.GetBytes(8)
	if string(bytes) != "test" {
		t.Errorf("Expected 'test', got '%s'", string(bytes))
	}

	page.SetInt(12, -98765)
	value = page.GetInt(12)
	if value != -98765 {
		t.Errorf("Expected %d, got %d", -98765, value)
	}

	page.SetString(16, "hello world!")
	s := page.GetString(16)
	if s != "hello world!" {
		t.Errorf("Expected 'hello world!', got '%s'", s)
	}
}
