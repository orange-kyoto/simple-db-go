package indexing

import (
	"simple-db-go/query"
	"simple-db-go/record"
)

type Index interface {
	BeforeFirst(searchKey query.Constant)

	Next() bool

	GetDataRecordID() record.RecordID

	Insert(val query.Constant, dataRecordID record.RecordID)

	Delete(val query.Constant, dataRecordID record.RecordID)

	Close()
}
