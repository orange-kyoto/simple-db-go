package file

import (
	"encoding/binary"
	"simple-db-go/types"
)

type Page struct {
	Data []byte
}

func NewPage(blocksize types.Int) *Page {
	return &Page{Data: make([]byte, blocksize)}
}

func NewPageFrom(b []byte) *Page {
	return &Page{Data: b}
}

func (p *Page) GetInt(offset types.Int) types.Int {
	return types.Int(binary.LittleEndian.Uint32(p.Data[offset:]))
}

func (p *Page) GetBytes(offset types.Int) []byte {
	length := p.GetInt(offset)
	blobOffset := offset + Int32ByteSize

	data := make([]byte, length)
	copy(data, p.Data[blobOffset:blobOffset+length])

	return data
}

func (p *Page) SetInt(offset types.Int, newData types.Int) {
	binary.LittleEndian.PutUint32(p.Data[offset:], uint32(newData))
}

func (p *Page) SetBytes(offset types.Int, newData []byte) {
	p.SetInt(offset, types.Int(len(newData)))
	blobOffset := offset + Int32ByteSize
	copy(p.Data[blobOffset:], newData)
}

func (p *Page) GetString(offset types.Int) string {
	b := p.GetBytes(offset)
	// 暗黙的にASCII文字列として扱う
	return string(b)
}

func (p *Page) SetString(offset types.Int, newData string) {
	bb := []byte(newData)
	p.SetBytes(offset, bb)
}

func MaxLength(strlen types.Int) types.Int {
	return Int32ByteSize + strlen*CharByteSize
}
