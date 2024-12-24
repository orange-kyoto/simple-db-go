package transaction

import "simple-db-go/buffer"

// TODO: 全体的に実装は後でやる！

type RecoveryManager struct {
}

func NewRecoveryManager() *RecoveryManager {
	return &RecoveryManager{}
}

func (rm *RecoveryManager) Commit()   {}
func (rm *RecoveryManager) Rollback() {}
func (rm *RecoveryManager) Recover()  {}

func (rm *RecoveryManager) SetInt(buffer *buffer.Buffer, offset int, newVal int) int       { return 0 }
func (rm *RecoveryManager) SetString(buffer *buffer.Buffer, offset int, newVal string) int { return 0 }
