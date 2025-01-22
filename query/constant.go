package query

import (
	"fmt"
	"simple-db-go/record"
	"simple-db-go/types"
)

func NewIntConstant(value types.Int) IntConstant {
	return IntConstant{value: value}
}

func NewStrConstant(value string) StrConstant {
	return StrConstant{value: value}
}

type IntConstant struct {
	value types.Int
}

func (ic IntConstant) GetValue() types.Int { return ic.value }

// For Constant interface
func (ic IntConstant) Constant()        {}
func (ic IntConstant) ToString() string { return ic.value.ToString() }

// For Expression interface
func (ic IntConstant) Evaluate(scan Scan) (Constant, error) { return ic, nil }
func (ic IntConstant) AppliesTo(schema *record.Schema) bool { return true }

type StrConstant struct {
	value string
}

func (sc StrConstant) GetValue() string { return sc.value }

// For Constant interface
func (sc StrConstant) Constant()        {}
func (sc StrConstant) ToString() string { return fmt.Sprintf("'%s'", sc.value) }

// For Expression interface
func (sc StrConstant) Evaluate(scan Scan) (Constant, error) { return sc, nil }
func (sc StrConstant) AppliesTo(schema *record.Schema) bool { return true }
