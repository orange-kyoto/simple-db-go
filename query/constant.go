package query

import "simple-db-go/types"

type Constant struct {
	intValue types.Int
	strValue string
	isInt    bool
}

func NewIntConstant(value types.Int) Constant {
	return Constant{intValue: value, isInt: true}
}

func NewStrConstant(value string) Constant {
	return Constant{strValue: value, isInt: false}
}

// TODO: intValue は０で初期化されるのでこれはまずい気がする。
func (c Constant) AsInt() types.Int {
	return c.intValue
}

// TODO: strValue は""で初期化されるのでこれはまずい気がする。
func (c Constant) AsString() string {
	return c.strValue
}

func (c Constant) ToString() string {
	if c.isInt {
		return c.intValue.ToString()
	} else {
		return c.strValue
	}
}
