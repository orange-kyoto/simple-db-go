package file

import (
	"encoding/binary"
	"errors"
)

const (
	// 32-bit 整数のバイトサイズ
	Int32ByteSize = 4
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

func (p *Page) GetInt(offset int) (int32, error) {
	if err := p.checkBounds(offset, Int32ByteSize); err != nil {
		return 0, err
	}

	return int32(binary.LittleEndian.Uint32(p.data[offset:])), nil
}

func (p *Page) GetBytes(offset int, length int) ([]byte, error) {
	if err := p.checkBounds(offset, length); err != nil {
		return nil, err
	}

	return p.data[offset : offset+length], nil
}

func (p *Page) SetInt(offset int, newData int32) error {
	if err := p.checkBounds(offset, Int32ByteSize); err != nil {
		return err
	}

	binary.LittleEndian.PutUint32(p.data[offset:], uint32(newData))
	return nil
}

func (p *Page) SetBytes(offset int, newData []byte) error {
	if err := p.checkBounds(offset, len(newData)); err != nil {
		return err
	}

	copy(p.data[offset:], newData)
	return nil
}

func (p *Page) checkBounds(offset int, length int) error {
	if offset < 0 || offset+length > len(p.data) {
		return errors.New("offset out of bounds")
	}

	return nil
}
