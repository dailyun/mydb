package store

import "fmt"

func DeleteRow(pager *Pager, rootPage int, key string) (int, error) {
	raw, err := pager.ReadPage(rootPage)
	if err != nil {
		return 0, fmt.Errorf("read rootPage %d: %w", rootPage, err)
	}
	page, err := PageFromBytes(raw)
	if err != nil {
		return 0, fmt.Errorf("parse rootPage: %w", err)
	}

	if page.Type == PageLeaf {
		found := false
		newCells := [][]byte{}
		for _, cell := range page.Cells {
			cellKey, err := ExtractKey(cell)
			if err != nil {
				continue
			}
			if cellKey == key {
				found = true
				continue
			}
			newCells = append(newCells, cell)
		}
		if !found {
			return 0, fmt.Errorf("not found")
		}
		page.Cells = newCells
		data, _ := page.ToBytes()
		err := pager.WritePage(rootPage, data)
		return rootPage, err
	}

	// Internal page
	childPage := int(page.LeftChild)
	for _, cell := range page.Cells {
		k, child, err := DecodeInternalCell(cell)
		if err != nil {
			continue
		}
		if key < k {
			break
		}
		childPage = int(child)
	}

	_, err = DeleteRow(pager, childPage, key)
	return rootPage, err

}
