package test

import (
	"fmt"
	"mySQLite/store"
	"testing"
)

func TestPager(t *testing.T) {
	pager, _ := store.OpenPager("data")

	// 写入一页
	data := make([]byte, store.PageSize)
	copy(data, []byte("Hello Page 1!"))
	pager.WritePage(1, data)

	// 读取该页
	read, _ := pager.ReadPage(1)
	fmt.Println(string(read[:20]))

	pager.Close()

}
