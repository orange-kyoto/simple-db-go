package grammar

import "simple-db-go/parsing/data"

var _ Statement = (*Commit)(nil)
var _ Statement = (*Rollback)(nil)

type Commit struct {
	Command string `"COMMIT" ";"?`
}

func (c *Commit) GrammarStatement() {}
func (c *Commit) ToData() data.SQLData {
	return &data.CommitData{}
}

type Rollback struct {
	Command string `"ROLLBACK" ";"?`
}

func (r *Rollback) GrammarStatement() {}
func (r *Rollback) ToData() data.SQLData {
	return &data.RollbackData{}
}
