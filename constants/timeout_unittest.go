//go:build test

package constants

import "time"

const (
	// テストの時は短い値に設定する.
	WAIT_THRESHOLD = 500 * time.Millisecond
)
