package server

import "fmt"

type InvalidDBNameError struct {
	dbName string
}

func (e InvalidDBNameError) Error() string {
	return fmt.Sprintf("無効なDB名です: %s", e.dbName)
}
