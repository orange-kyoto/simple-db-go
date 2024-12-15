package file

import (
	"encoding/binary"
)

const (
	// 32-bit 整数のバイトサイズ
	Int32ByteSize = 4
	// ASCII文字のバイト数
	CharByteSize = 1
)

type Page struct {
	data []byte
}

func NewPage(blocksize int) *Page {
	return &Page{data: make([]byte, blocksize)}
}

func NewPageFrom(b []byte) *Page {
	return &Page{data: b}
}

func (p *Page) GetInt(offset int) int32 {
	return int32(binary.LittleEndian.Uint32(p.data[offset:]))
}

func (p *Page) GetBytes(offset int) []byte {
	length := p.GetInt(offset)
	blobOffset := offset + Int32ByteSize

	return p.data[blobOffset : blobOffset+int(length)]
}

func (p *Page) SetInt(offset int, newData int32) {
	binary.LittleEndian.PutUint32(p.data[offset:], uint32(newData))
}

func (p *Page) SetBytes(offset int, newData []byte) {
	p.SetInt(offset, int32(len(newData)))
	blobOffset := offset + Int32ByteSize
	copy(p.data[blobOffset:], newData)
}

func (p *Page) GetString(offset int) string {
	b := p.GetBytes(offset)
	// 暗黙的にASCII文字列として扱う
	return string(b)
}

func (p *Page) SetString(offset int, newData string) {
	bb := []byte(newData)
	p.SetBytes(offset, bb)
}

func MaxLength(strlen int) int {
	return Int32ByteSize + strlen*CharByteSize
}
