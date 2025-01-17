package record

import "simple-db-go/types"

func NewIntConstant(value types.Int) IntConstant {
	return IntConstant{value: value}
}

func NewStrConstant(value string) StrConstant {
	return StrConstant{value: value}
}

type IntConstant struct {
	value types.Int
}

func (ic IntConstant) Constant() {}

func (ic IntConstant) ToString() string { return ic.value.ToString() }

type StrConstant struct {
	value string
}

func (sc StrConstant) Constant() {}

func (sc StrConstant) ToString() string { return sc.value }
