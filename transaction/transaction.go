package transaction

import (
	"simple-db-go/file"
	"simple-db-go/types"
)

// TODO: 全体的に実装は後でやる！

type Transaction struct {
}

func NewTransaction() *Transaction {
	return &Transaction{}
}

func (t *Transaction) Commit()   {}
func (t *Transaction) Rollback() {}
func (t *Transaction) Recover()  {}

func (t *Transaction) Pin(blockID *file.BlockID)   {}
func (t *Transaction) Unpin(blockID *file.BlockID) {}

func (t *Transaction) GetInt(blockID *file.BlockID, offset types.Int) types.Int                    { return 0 }
func (t *Transaction) GetString(blockID *file.BlockID, offset types.Int) string                    { return "" }
func (t *Transaction) SetInt(blockID *file.BlockID, offset types.Int, val types.Int, okToLog bool) {}
func (t *Transaction) SetString(blockID *file.BlockID, offset types.Int, val string, okToLog bool) {}

func (t *Transaction) AvailableBuffers() types.Int          { return 0 }
func (t *Transaction) Size(filename string) types.Int       { return 0 }
func (t *Transaction) Append(filename string) *file.BlockID { return nil }
func (t *Transaction) BlockSize() types.Int                 { return 0 }
