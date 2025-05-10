package db

import (
	"fmt"
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
	visited := map[int]bool{}
	return collectRecursive(pager, rootPage, visited)
}

func collectRecursive(pager *store.Pager, pageNo int, visited map[int]bool) ([][]string, error) {
	if visited[pageNo] {
		return nil, fmt.Errorf("⚠️ detected loop: page %d already visited", pageNo)
	}
	visited[pageNo] = true

	raw, err := pager.ReadPage(pageNo)
	if err != nil {
		return nil, err
	}
	page, err := store.PageFromBytes(raw)
	if err != nil {
		return nil, err
	}

	rows := [][]string{}

	if page.Type == store.PageLeaf {
		for _, cell := range page.Cells {
			r, err := store.DecodeRow(cell)
			if err != nil {
				continue
			}
			rows = append(rows, r)
		}
		if page.NextLeaf != 0 {
			nextRows, err := collectRecursive(pager, int(page.NextLeaf), visited)
			if err != nil {
				return nil, err
			}
			rows = append(rows, nextRows...)
		}
		return rows, nil
	}

	// Internal page
	leftRows, err := collectRecursive(pager, int(page.LeftChild), visited)
	if err == nil {
		rows = append(rows, leftRows...)
	}
	for _, cell := range page.Cells {
		_, child, err := store.DecodeInternalCell(cell)
		if err != nil {
			continue
		}
		childRows, err := collectRecursive(pager, int(child), visited)
		if err == nil {
			rows = append(rows, childRows...)
		}
	}
	return rows, nil
}
