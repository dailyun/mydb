package store

import "fmt"

func SearchRow(pager *Pager, rootPage int, key string) ([]byte, error) {
	if rootPage <= 0 {
		return nil, fmt.Errorf("invalid rootPage: %d", rootPage)
	}

	raw, err := pager.ReadPage(rootPage)
	if err != nil {
		return nil, fmt.Errorf("read rootPage %d: %w", rootPage, err)
	}
	page, err := PageFromBytes(raw)
	if err != nil {
		return nil, fmt.Errorf("parse rootPage: %w", err)
	}

	switch page.Type {
	case PageLeaf:
		for _, cell := range page.Cells {
			cellKey, err := ExtractKey(cell)
			if err != nil {
				continue
			}
			if cellKey == key {
				return cell, nil
			}
		}
		return nil, fmt.Errorf("key %s not found", key)

	case PageInternal:
		// binary search 内节点
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
		return SearchRow(pager, childPage, key)

	default:
		return nil, fmt.Errorf("invalid page type: %d", page.Type)
	}
}
