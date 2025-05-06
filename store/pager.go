package store

import (
	"fmt"
	"os"
)

const PageSize = 4096

type Pager struct {
	file     *os.File
	filename string
	nextPage int
}

// OpenPager 打开数据库文件
func OpenPager(filename string) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	info, _ := file.Stat()
	pageCount := int(info.Size()) / PageSize
	if pageCount == 0 {
		pageCount = 1 // 至少从 1 开始
	}
	return &Pager{file: file, filename: filename, nextPage: pageCount + 1}, nil
}

// ReadPage 读取页
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

// WritePage 写入页
func (p *Pager) WritePage(pageNum int, data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("data length must be equal to PageSize")
	}
	offset := int64((pageNum - 1) * PageSize)
	_, err := p.file.WriteAt(data, offset)
	return err
}

// Close 关闭文件
func (p *Pager) Close() error {
	return p.file.Close()

}

func (p *Pager) AllocatePage() int {
	page := p.nextPage
	p.nextPage++
	return page
}
