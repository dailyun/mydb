package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

/*
| 字节位置 | 内容                      |
| ---- | ----------------------- |
| 0    | 类型 (0x0D)               |
| 1-2  | cell 数                  |
| 3-4  | cell 起始偏移               |
| 5-6  | 下一页页号                   |
| 7+   | cell offset + cell data |

*/

/*
| 字节位置 | 内容                                   |
| ---- | ------------------------------------ |
| 0    | 类型 (0x05)                            |
| 1-2  | cell 数                               |
| 3-4  | cell 起始偏移                            |
| 5-8  | 首子页页号（leftmost）                      |
| 9+   | cell offset + \[key, childPage] cell |
*/

const (
	PageLeaf     = 0x0D
	PageInternal = 0x05
)

type Page struct {
	Type      byte
	Cells     [][]byte
	Offsets   []uint16
	NextLeaf  uint32
	LeftChild uint32
}

type InsertResult struct {
	SelfChanged bool
	Split       bool
	NewPage     *Page
	NewPageNo   int
}

// 创建一个空的叶子页
func NewLeafPage() *Page {
	return &Page{
		Type:    PageLeaf,
		Cells:   [][]byte{},
		Offsets: []uint16{},
	}
}
func NewInternalPage() *Page {
	return &Page{
		Type:    PageInternal,
		Cells:   [][]byte{},
		Offsets: []uint16{},
	}
}

// 提取 key：取 row 的第一个字段
func ExtractKey(record []byte) (string, error) {
	fields, err := DecodeRow(record)
	if err != nil || len(fields) == 0 {
		return "", fmt.Errorf("invalid row")
	}
	return fields[0], nil
}

// 编码 InternalPage 的 cell：key(string) + childPage(uint32)
func EncodeInternalCell(key string, childPage uint32) []byte {
	buf := new(bytes.Buffer)
	buf.WriteString(key)
	buf.WriteByte(0)
	binary.Write(buf, binary.LittleEndian, childPage)
	return buf.Bytes()
}

func DecodeInternalCell(data []byte) (string, uint32, error) {
	nullIndex := bytes.IndexByte(data, 0)
	if nullIndex == -1 || nullIndex+5 > len(data) {
		return "", 0, fmt.Errorf("invalid internal cell: nullIndex=%d, len=%d", nullIndex, len(data))
	}
	key := string(data[:nullIndex])
	child := binary.LittleEndian.Uint32(data[nullIndex+1 : nullIndex+5])
	return key, child, nil
}

// ToBytes page to bytes
func (p *Page) ToBytes() ([]byte, error) {
	buf := make([]byte, PageSize)
	buf[0] = p.Type
	cellCount := uint16(len(p.Cells))
	binary.LittleEndian.PutUint16(buf[1:], cellCount)

	offset := PageSize
	p.Offsets = make([]uint16, 0, cellCount)

	// 分配 cell 区（从页尾向前）
	for i := 0; i < int(cellCount); i++ {
		cell := p.Cells[i]
		offset -= len(cell)
		// ✳️ 确保 offset 不越过 header(9 字节) + 每条 cell 2 字节指针表
		if offset < 9+int(cellCount)*2 {
			return nil, fmt.Errorf("ToBytes: page overflow at cell %d, offset=%d", i, offset)
		}

		copy(buf[offset:], cell)
		p.Offsets = append(p.Offsets, uint16(offset))
	}

	binary.LittleEndian.PutUint16(buf[3:], uint16(offset)) // cell 内容区起始

	// 写特殊字段（4 字节）
	if p.Type == PageLeaf {
		binary.LittleEndian.PutUint32(buf[5:], p.NextLeaf)
	} else {
		binary.LittleEndian.PutUint32(buf[5:], p.LeftChild)
	}

	// 写 cell pointer 表（header 9 字节后开始）
	tableStart := 9
	for i := 0; i < int(cellCount); i++ {
		binary.LittleEndian.PutUint16(buf[tableStart+i*2:], p.Offsets[i])
	}

	return buf, nil
}

// FromBytes bytes to page
func PageFromBytes(data []byte) (*Page, error) {
	if len(data) != PageSize {
		return nil, errors.New("invalid page size")
	}
	typ := data[0]
	cellCount := binary.LittleEndian.Uint16(data[1:])
	offsets := make([]uint16, cellCount)
	for i := 0; i < int(cellCount); i++ {
		offsets[i] = binary.LittleEndian.Uint16(data[9+i*2:])
	}
	cells := make([][]byte, cellCount)
	for i, off := range offsets {
		var end int
		if i == 0 {
			end = PageSize
		} else {
			end = int(offsets[i-1])
		}
		cells[i] = data[int(off):end]
	}

	page := &Page{
		Type:    typ,
		Cells:   cells,
		Offsets: offsets,
	}
	if typ == PageLeaf {
		page.NextLeaf = binary.LittleEndian.Uint32(data[5:])
	} else if typ == PageInternal {
		page.LeftChild = binary.LittleEndian.Uint32(data[5:])
	}
	return page, nil
}
