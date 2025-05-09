package store

import "fmt"

func InsertIntoLeafPage(pager *Pager, pageNo int, row []byte) (InsertResult, error) {
	raw, err := pager.ReadPage(pageNo)
	if err != nil {
		return InsertResult{}, err
	}
	page, err := PageFromBytes(raw)
	if err != nil {
		return InsertResult{}, err
	}
	if page.Type != PageLeaf {
		return InsertResult{}, fmt.Errorf("InsertIntoLeafPage: not a leaf page")
	}

	// 试图添加新记录后模拟 ToBytes，看是否仍能容纳
	test := *page // 浅拷贝
	test.Cells = append(test.Cells, row)
	test.Offsets = nil // 让 ToBytes 重新计算
	if _, err := test.ToBytes(); err == nil {
		// 容纳得下
		page.Cells = append(page.Cells, row)
		data, _ := page.ToBytes()
		pager.WritePage(pageNo, data)
		return InsertResult{SelfChanged: true, Split: false}, nil
	}

	// 页满，执行分裂
	page.Cells = append(page.Cells, row) // 加回来再分裂

	mid := len(page.Cells) / 2
	left := NewLeafPage()
	right := NewLeafPage()

	left.Cells = append(left.Cells, page.Cells[:mid]...)
	right.Cells = append(right.Cells, page.Cells[mid:]...)

	rightPage := pager.AllocatePage()
	left.NextLeaf = uint32(rightPage)

	dataL, _ := left.ToBytes()
	dataR, _ := right.ToBytes()
	pager.WritePage(pageNo, dataL)
	pager.WritePage(rightPage, dataR)

	return InsertResult{
		SelfChanged: true,
		Split:       true,
		NewPage:     right,
		NewPageNo:   rightPage,
	}, nil
}
func InsertRow(pager *Pager, rootPage int, row []byte) (int, error) {
	if rootPage <= 0 {
		return 0, fmt.Errorf("invalid rootPage: %d", rootPage)
	}

	raw, err := pager.ReadPage(rootPage)
	if err != nil {
		return 0, fmt.Errorf("read rootPage %d: %w", rootPage, err)
	}
	root, err := PageFromBytes(raw)
	if err != nil {
		return 0, fmt.Errorf("parse rootPage: %w", err)
	}

	// ✅ CASE 1: 插入到叶子页
	if root.Type == PageLeaf {
		res, err := InsertIntoLeafPage(pager, rootPage, row)
		if err != nil {
			return 0, err
		}
		if !res.Split {
			return rootPage, nil
		}

		// ➤ 分裂，生成新 root
		key, err := ExtractKey(res.NewPage.Cells[0])
		if err != nil {
			return 0, fmt.Errorf("ExtractKey on new right leaf failed: %w", err)
		}
		newRoot := NewInternalPage()
		newRoot.LeftChild = uint32(rootPage)
		newRoot.Cells = append(newRoot.Cells, EncodeInternalCell(key, uint32(res.NewPageNo)))
		sortInternalCells(newRoot) // ✅ 确保 newRoot 也是有序的！
		data, err := newRoot.ToBytes()
		if err != nil {
			return 0, fmt.Errorf("failed to encode new root: %w", err)
		}
		newRootPage := pager.AllocatePage()
		pager.WritePage(newRootPage, data)
		return newRootPage, nil
	}

	// ✅ CASE 2: Internal Page → 递归下插
	insertPage := int(root.LeftChild)

	rowKey, err := ExtractKey(row)
	if err != nil {
		return 0, fmt.Errorf("extract key from row: %w", err)
	}

	var i int
	for i = 0; i < len(root.Cells); i++ {
		key, child, err := DecodeInternalCell(root.Cells[i])
		if err != nil {
			return 0, fmt.Errorf("DecodeInternalCell at i=%d: %w", i, err)
		}
		if rowKey < key {
			break
		}
		insertPage = int(child)
	}

	newChildPage, err := InsertRow(pager, insertPage, row)
	if err != nil {
		return 0, err
	}
	if newChildPage == insertPage {
		return rootPage, nil
	}

	// ✅ 子页发生分裂：插入新的 key + newPageNo
	newChildRaw, err := pager.ReadPage(newChildPage)
	if err != nil {
		return 0, err
	}
	newChild, err := PageFromBytes(newChildRaw)
	if err != nil {
		return 0, err
	}
	midKey, _, err := DecodeInternalCell(newChild.Cells[0])
	if err != nil {
		return 0, fmt.Errorf("ExtractKey failed for promotion: %w", err)
	}
	newCell := EncodeInternalCell(midKey, uint32(newChildPage))

	root.Cells = append(root.Cells, newCell)
	sortInternalCells(root) // ✅ 排序 root
	root.Offsets = nil      // 重算 offset

	if _, err := root.ToBytes(); err == nil {
		data, _ := root.ToBytes()
		pager.WritePage(rootPage, data)
		return rootPage, nil
	}

	// ⚠️ 当前 InternalPage 也满了 → 分裂
	mid := len(root.Cells) / 2
	left := NewInternalPage()
	right := NewInternalPage()
	left.LeftChild = root.LeftChild
	left.Cells = append(left.Cells, root.Cells[:mid]...)
	right.LeftChild = decodeRightMostChild(root, mid)
	right.Cells = append(right.Cells, root.Cells[mid+1:]...)

	sortInternalCells(left)
	sortInternalCells(right)

	rightPage := pager.AllocatePage()
	pager.WritePage(rootPage, left.ToBytesMust())
	pager.WritePage(rightPage, right.ToBytesMust())

	promoteKey, _, err := DecodeInternalCell(root.Cells[mid])
	if err != nil {
		return 0, fmt.Errorf("decode promoteKey: %w", err)
	}

	newRoot := NewInternalPage()
	newRoot.LeftChild = uint32(rootPage)
	newRoot.Cells = append(newRoot.Cells, EncodeInternalCell(promoteKey, uint32(rightPage)))
	sortInternalCells(newRoot) // ✅ 排序 newRoot too!
	newRootPage := pager.AllocatePage()
	pager.WritePage(newRootPage, newRoot.ToBytesMust())
	return newRootPage, nil
}

func decodeRightMostChild(p *Page, idx int) uint32 {
	if idx >= len(p.Cells) {
		return 0
	}
	_, child, err := DecodeInternalCell(p.Cells[idx])
	if err != nil {
		return 0
	}
	return child
}

func (p *Page) ToBytesMust() []byte {
	b, err := p.ToBytes()
	if err != nil {
		panic(err)
	}
	return b
}
