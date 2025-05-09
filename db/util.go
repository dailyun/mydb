package db

import (
	"mySQLite/store"
)

func padToPage(data []byte) []byte {
	padded := make([]byte, store.PageSize)
	copy(padded, data)
	return padded
}

func trimPadding(data []byte) []byte {
	end := len(data)
	for end > 0 && data[end-1] == 0 {
		end--
	}
	return data[:end]
}

func collectRowsFromTree(pager *store.Pager, rootPage int) ([][]string, error) {
	pageData, err := pager.ReadPage(rootPage)
	if err != nil {
		return nil, err
	}
	page, err := store.PageFromBytes(pageData)
	if err != nil {
		return nil, err
	}

	if page.Type == store.PageLeaf {
		rows := [][]string{}
		for _, cell := range page.Cells {
			row, err := store.DecodeRow(cell)
			if err != nil {
				continue
			}
			rows = append(rows, row)
		}
		// 如果有 下一页，则继续递归
		if page.NextLeaf != 0 {
			nextRows, err := collectRowsFromTree(pager, int(page.NextLeaf))
			if err == nil {
				rows = append(rows, nextRows...)
			}
		}
		return rows, nil
	}

	// 如果是内部节点，则递归处理
	rows := [][]string{}
	left := int(page.LeftChild)
	subRows, err := collectRowsFromTree(pager, left)
	if err == nil {
		rows = append(rows, subRows...)
	}
	for _, cell := range page.Cells {
		_, child, err := store.DecodeInternalCell(cell)
		if err != nil {
			continue
		}
		subRows, err := collectRowsFromTree(pager, int(child))
		if err == nil {
			rows = append(rows, subRows...)
		}
	}
	return rows, nil
}
