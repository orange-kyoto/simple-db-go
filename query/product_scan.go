package query

import (
	"simple-db-go/types"
)

type ProductScan struct {
	scan1 Scan
	scan2 Scan
}

func NewProductScan(scan1 Scan, scan2 Scan) *ProductScan {
	defer scan1.Next()
	return &ProductScan{scan1: scan1, scan2: scan2}
}

func (ps *ProductScan) BeforeFirst() {
	ps.scan1.BeforeFirst()
	ps.scan1.Next()
	ps.scan2.BeforeFirst()
}

func (ps *ProductScan) Next() bool {
	if ps.scan2.Next() {
		return true
	} else {
		ps.scan2.BeforeFirst()
		return ps.scan2.Next() && ps.scan1.Next()
	}
}

func (ps *ProductScan) GetInt(fieldName types.FieldName) (types.Int, error) {
	if ps.scan1.HasField(fieldName) {
		return ps.scan1.GetInt(fieldName)
	} else {
		return ps.scan2.GetInt(fieldName)
	}
}

func (ps *ProductScan) GetString(fieldName types.FieldName) (string, error) {
	if ps.scan1.HasField(fieldName) {
		return ps.scan1.GetString(fieldName)
	} else {
		return ps.scan2.GetString(fieldName)
	}
}

func (ps *ProductScan) GetValue(fieldName types.FieldName) (Constant, error) {
	if ps.scan1.HasField(fieldName) {
		return ps.scan1.GetValue(fieldName)
	} else {
		return ps.scan2.GetValue(fieldName)
	}
}

func (ps *ProductScan) HasField(fieldName types.FieldName) bool {
	return ps.scan1.HasField(fieldName) || ps.scan2.HasField(fieldName)
}

func (ps *ProductScan) Close() {
	ps.scan1.Close()
	ps.scan2.Close()
}
