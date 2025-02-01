package query

import (
	"simple-db-go/types"
)

var _ Scan = (*ProjectScan)(nil)

type ProjectScan struct {
	scan          Scan
	fieldNameList []types.FieldName
}

func NewProjectScan(scan Scan, fieldNameList []types.FieldName) *ProjectScan {
	return &ProjectScan{scan: scan, fieldNameList: fieldNameList}
}

func (ps *ProjectScan) BeforeFirst() {
	ps.scan.BeforeFirst()
}

func (ps *ProjectScan) Next() bool {
	return ps.scan.Next()
}

func (ps *ProjectScan) GetInt(fieldName types.FieldName) (types.Int, error) {
	if ps.HasField(fieldName) {
		return ps.scan.GetInt(fieldName)
	} else {
		return 0, &UnknownFieldInProjectScanError{fieldName, ps}
	}
}

func (ps *ProjectScan) GetString(fieldName types.FieldName) (string, error) {
	if ps.HasField(fieldName) {
		return ps.scan.GetString(fieldName)
	} else {
		return "", &UnknownFieldInProjectScanError{fieldName, ps}
	}
}

func (ps *ProjectScan) GetValue(fieldName types.FieldName) (Constant, error) {
	if ps.HasField(fieldName) {
		return ps.scan.GetValue(fieldName)
	} else {
		return nil, &UnknownFieldInProjectScanError{fieldName, ps}
	}
}

func (ps *ProjectScan) HasField(fieldName types.FieldName) bool {
	for _, f := range ps.fieldNameList {
		if f == fieldName {
			return true
		}
	}
	return false
}

func (ps *ProjectScan) Close() {
	ps.scan.Close()
}

func (ps *ProjectScan) GetFields() []types.FieldName {
	return ps.fieldNameList
}
