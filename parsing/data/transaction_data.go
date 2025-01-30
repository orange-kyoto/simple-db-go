package data

var _ SQLData = (*CommitData)(nil)
var _ SQLData = (*RollbackData)(nil)

type CommitData struct{}

func (*CommitData) SQLData() {}

type RollbackData struct{}

func (*RollbackData) SQLData() {}
