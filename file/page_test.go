package file

import "testing"

func TestPage(t *testing.T) {
	page := NewPage(4096)

	err := page.SetInt(4, 12345)

	value, err := page.GetInt(4)
	if err != nil || value != 12345 {
		t.Errorf("Expected %d, got %d, error: %v", 12345, value, err)
	}

	err = page.SetBytes(8, []byte("test"))
	if err != nil {
		t.Errorf("Failed to SetBytes: %v", err)
	}

	bytes, err := page.GetBytes(8, 4)
	if err != nil || string(bytes) != "test" {
		t.Errorf("Expected 'test', got '%s', error: %v", string(bytes), err)
	}

	err = page.SetInt(12, -98765)
	value, err = page.GetInt(12)
	if err != nil || value != -98765 {
		t.Errorf("Expected %d, got %d, error: %v", -98765, value, err)
	}
}
