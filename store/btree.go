package store

import (
	"encoding/binary"
	"fmt"
)

/*
| 偏移  | 大小  | 说明              |
| --- | --- | --------------- |
| 0   | 1   | 页类型（0x0D 表示叶子页） |
| 1-2 | 2   | cell 数          |
| 3-4 | 2   | cell 区起始位置      |
| 5-6 | 2   | 空闲空间起始（可省略）     |
| 7\~ | 2N  | 每个 cell 的偏移地址   |
| ... | N/A | cell 内容，从页尾向前写  |
*/

const (
	PageLeaf = 0x0D
)

type Page struct {
	Type    byte
	Cells   [][]byte
	Offsets []uint16
}

// 创建一个空的叶子页
func NewLeafPage() *Page {
	return &Page{
		Type:    PageLeaf,
		Cells:   [][]byte{},
		Offsets: []uint16{},
	}
}

// NewPage returns a new page with the given type
func NewPage(pageType byte) *Page {
	return &Page{
		Type:    pageType,
		Cells:   make([][]byte, 0),
		Offsets: make([]uint16, 0),
	}
}

// InsertCell  append cell to page
func (p *Page) InsertCell(cell []byte) error {
	headerSize := 7 + 2*len(p.Cells)
	cellSpace := 0
	for _, cell := range p.Cells {
		cellSpace += len(cell)
	}
	remaining := PageSize - headerSize - cellSpace - len(cell)
	if remaining < 0 {
		return fmt.Errorf("cell size too large")
	}
	p.Cells = append(p.Cells, cell)
	return nil
}

// ToBytes page to bytes
func (p *Page) ToBytes() ([]byte, error) {
	buf := make([]byte, PageSize)
	buf[0] = p.Type
	cellCount := len(p.Cells)
	binary.LittleEndian.PutUint16(buf[1:], uint16(cellCount))

	offset := PageSize
	p.Offsets = make([]uint16, 0, cellCount) // ✅ 清空偏移

	for i := 0; i < int(cellCount); i++ {
		cell := p.Cells[i]
		offset -= len(cell)
		copy(buf[offset:], cell)
		p.Offsets = append(p.Offsets, uint16(offset))
	}

	//写入偏移量
	tableStart := 7
	for i := 0; i < int(cellCount); i++ {
		binary.LittleEndian.PutUint16(buf[tableStart+i*2:], p.Offsets[i])
	}
	//起始cell 偏移量
	binary.LittleEndian.PutUint16(buf[3:], uint16(offset))

	return buf, nil
}

// FromBytes bytes to page
func PageFromBytes(data []byte) (*Page, error) {
	if len(data) != PageSize {
		return nil, fmt.Errorf("page size must be %d", PageSize)
	}
	if data[0] != PageLeaf {
		return nil, fmt.Errorf("page type must be %d", PageLeaf)
	}
	cellCount := binary.LittleEndian.Uint16(data[1:])
	offsets := make([]uint16, cellCount)
	for i := 0; i < int(cellCount); i++ {
		offsets[i] = binary.LittleEndian.Uint16(data[7+i*2:])
	}
	cells := make([][]byte, cellCount)
	for i := 0; i < int(cellCount); i++ {
		var end int
		if i == 0 {
			end = PageSize
		} else {
			end = int(offsets[i-1])
		}
		cells[i] = data[offsets[i]:end]
	}
	return &Page{
		Type:    data[0],
		Cells:   cells,
		Offsets: offsets,
	}, nil
}
