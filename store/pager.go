// 文件: store/pager.go
package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

const PageSize = 4096

type Pager struct {
	file     *os.File
	filename string
	nextPage int
}

func OpenPager(filename string) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	size := info.Size()
	pageCount := int(size / PageSize)
	if size%PageSize != 0 {
		pageCount++ // 补上残页
	}
	if pageCount == 0 {
		pageCount = 1 // 至少一页起步
		// 初始化页1为全0
		empty := make([]byte, PageSize)
		_, err := file.WriteAt(empty, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to write initial page 1: %w", err)
		}
	}

	fmt.Printf("[Pager] Database has %d pages\n", pageCount)

	return &Pager{
		file:     file,
		filename: filename,
		nextPage: pageCount + 1, // 下一可分配页号
	}, nil
}

func (p *Pager) ReadPage(pageNum int) ([]byte, error) {
	if pageNum < 1 {
		return nil, fmt.Errorf("pageNum must be greater than 0")
	}
	offset := int64((pageNum - 1) * PageSize)
	data := make([]byte, PageSize)
	_, err := p.file.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (p *Pager) WritePage(pageNum int, data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("data length must be equal to PageSize")
	}
	offset := int64((pageNum - 1) * PageSize)
	_, err := p.file.WriteAt(data, offset)
	if err != nil {
		return err
	}
	return p.file.Sync()
}

func (p *Pager) Close() error {
	return p.file.Close()
}

func (p *Pager) AppendRow(pageNum int, row []byte) error {
	page, err := p.ReadPage(pageNum)
	if err != nil || bytes.Equal(page, make([]byte, PageSize)) {
		page = make([]byte, PageSize)
		binary.LittleEndian.PutUint32(page, 0)
	}
	existing := page
	count := int(binary.LittleEndian.Uint32(existing[0:4]))
	offset := 4
	for i := 0; i < count; i++ {
		if offset+4 > PageSize {
			return fmt.Errorf("offset overflow reading length")
		}
		rowLen := int(binary.LittleEndian.Uint32(existing[offset:]))
		offset += 4 + rowLen
	}

	if offset+4+len(row) > PageSize {
		return fmt.Errorf("page full, can't append row of length %d at offset %d", len(row), offset)
	}

	binary.LittleEndian.PutUint32(existing[offset:], uint32(len(row)))
	copy(existing[offset+4:], row)
	binary.LittleEndian.PutUint32(existing[0:], uint32(count+1))

	return p.WritePage(pageNum, existing)
}

func (p *Pager) ReadAllRows(pageNum int) ([][]byte, error) {
	page, err := p.ReadPage(pageNum)
	if err != nil {
		return nil, err
	}
	count := int(binary.LittleEndian.Uint32(page[0:4]))
	rows := [][]byte{}
	offset := 4
	for i := 0; i < count; i++ {
		if offset+4 > PageSize {
			break
		}
		rowLen := int(binary.LittleEndian.Uint32(page[offset:]))
		offset += 4
		if offset+rowLen > PageSize {
			break
		}
		rows = append(rows, page[offset:offset+rowLen])
		offset += rowLen
	}
	return rows, nil
}

func (p *Pager) AllocatePage() int {
	if p.nextPage < 2 {
		p.nextPage = 2 // 保留页 1 用于元数据表（sqlite_master）
	}
	page := p.nextPage
	p.nextPage++
	return page
}
