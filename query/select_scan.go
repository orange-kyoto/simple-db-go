package query

import (
	"fmt"
	"simple-db-go/record"
	"simple-db-go/types"
)

type SelectScan struct {
	scan      Scan
	predicate *Predicate
}

func NewSelectScan(scan Scan, predicate *Predicate) *SelectScan {
	return &SelectScan{scan: scan, predicate: predicate}
}

// ----------------------------------------
// Methods of Scan Interface
// ----------------------------------------

func (s *SelectScan) BeforeFirst() {
	s.scan.BeforeFirst()
}

// NOTE: 与えられたpredicateが満たされるまでscanを進める.
func (s *SelectScan) Next() bool {
	for s.scan.Next() {
		isSatisfied, err := s.predicate.IsSatisfied(s.scan)
		if err != nil {
			// このエラーがあるなら、Next() も (bool, error) を返すように変更するべきかもしれない.
			// 一旦保留にする。無効な列名を指定した時のエラーをきちんとハンドリングするならエラーを返すべき.
			// TODO: Scan.Next() の仕様変更
			panic(fmt.Sprintf("[SelectScan] predicate.IsSatisfied でエラーが発生しました。 predicate=%+v, scan=%+v, error=%+v", s.predicate, s.scan, err))
		}
		if isSatisfied {
			return true
		}
	}
	return false
}

func (s *SelectScan) GetInt(fieldName types.FieldName) (types.Int, error) {
	return s.scan.GetInt(fieldName)
}

func (s *SelectScan) GetString(fieldName types.FieldName) (string, error) {
	return s.scan.GetString(fieldName)
}

func (s *SelectScan) GetValue(fieldName types.FieldName) (record.Constant, error) {
	return s.scan.GetValue(fieldName)
}

func (s *SelectScan) HasField(fieldName types.FieldName) bool {
	return s.scan.HasField(fieldName)
}

func (s *SelectScan) Close() {
	s.scan.Close()
}

// ----------------------------------------
// Methods of UpdateScan Interface
// ----------------------------------------

func (s *SelectScan) SetInt(fieldName types.FieldName, value types.Int) error {
	// SimpleDB の update planner は table scan, select scan しか関係しないので、特にエラーをハンドリングしない。
	if s, ok := s.scan.(UpdateScan); ok {
		return s.SetInt(fieldName, value)
	} else {
		panic(fmt.Sprintf("[SelectScan] updatable でない scan で SetInt が呼ばれました。 select_scan=%+v, field_name=%s, value=%d", s, fieldName, value))
	}
}

func (s *SelectScan) SetString(fieldName types.FieldName, value string) error {
	// SimpleDB の update planner は table scan, select scan しか関係しないので、特にエラーをハンドリングしない。
	if s, ok := s.scan.(UpdateScan); ok {
		return s.SetString(fieldName, value)
	} else {
		panic(fmt.Sprintf("[SelectScan] updatable でない scan で SetString が呼ばれました。 select_scan=%+v, field_name=%s, value=%s", s, fieldName, value))
	}
}

func (s *SelectScan) SetValue(fieldName types.FieldName, value record.Constant) error {
	// SimpleDB の update planner は table scan, select scan しか関係しないので、特にエラーをハンドリングしない。
	if s, ok := s.scan.(UpdateScan); ok {
		return s.SetValue(fieldName, value)
	} else {
		panic(fmt.Sprintf("[SelectScan] updatable でない scan で SetValue が呼ばれました。 select_scan=%+v, field_name=%s, value=%+v", s, fieldName, value))
	}
}

func (s *SelectScan) Insert() {
	// SimpleDB の update planner は table scan, select scan しか関係しないので、特にエラーをハンドリングしない。
	if s, ok := s.scan.(UpdateScan); ok {
		s.Insert()
	} else {
		panic(fmt.Sprintf("[SelectScan] updatable でない scan で Insert が呼ばれました。 select_scan=%+v", s))
	}
}

func (s *SelectScan) Delete() {
	// SimpleDB の update planner は table scan, select scan しか関係しないので、特にエラーをハンドリングしない。
	if s, ok := s.scan.(UpdateScan); ok {
		s.Delete()
	} else {
		panic(fmt.Sprintf("[SelectScan] updatable でない scan で Delete が呼ばれました。 select_scan=%+v", s))
	}
}

func (s *SelectScan) GetCurrentRecordID() record.RecordID {
	if s, ok := s.scan.(UpdateScan); ok {
		return s.GetCurrentRecordID()
	} else {
		panic(fmt.Sprintf("[SelectScan] updatable でない scan で GetCurrentRecordID が呼ばれました。 select_scan=%+v", s))
	}
}

func (s *SelectScan) MoveToRecordID(recordID record.RecordID) {
	if s, ok := s.scan.(UpdateScan); ok {
		s.MoveToRecordID(recordID)
	} else {
		panic(fmt.Sprintf("[SelectScan] updatable でない scan で MoveToRecordID が呼ばれました。 select_scan=%+v, record_id=%+v", s, recordID))
	}
}
