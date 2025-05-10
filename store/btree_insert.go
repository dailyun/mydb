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

	// ✅ CASE 1: 叶子页插入
	if root.Type == PageLeaf {
		res, err := InsertIntoLeafPage(pager, rootPage, row)
		if err != nil {
			return 0, err
		}
		if !res.Split {
			return rootPage, nil
		}

		// ⚠️ promote key 和 childPage = res.NewPageNo
		key, err := ExtractKey(res.NewPage.Cells[0])
		if err != nil {
			return 0, fmt.Errorf("ExtractKey on right split leaf failed: %w", err)
		}

		newRoot := NewInternalPage()
		newRoot.LeftChild = uint32(rootPage)
		newRoot.Cells = append(newRoot.Cells, EncodeInternalCell(key, uint32(res.NewPageNo)))
		sortInternalCells(newRoot)

		newRootPage := pager.AllocatePage()
		pager.WritePage(newRootPage, newRoot.ToBytesMust())
		return newRootPage, nil
	}

	// ✅ CASE 2: Internal Page → 向下递归插入
	insertPage := int(root.LeftChild)
	rowKey, err := ExtractKey(row)
	if err != nil {
		return 0, fmt.Errorf("extract key from row: %w", err)
	}
	for i := 0; i < len(root.Cells); i++ {
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

	// ✅ promoteKey: 从 newChild 的第一个 key 抽取
	newChildRaw, err := pager.ReadPage(newChildPage)
	if err != nil {
		return 0, err
	}
	newChild, err := PageFromBytes(newChildRaw)
	if err != nil {
		return 0, err
	}
	// ✅ promoteKey, promoteChild 来自 newChild 的第一个 InternalCell
	var promoteKey string
	var promoteChild uint32

	if newChild.Type == PageLeaf {
		promoteKey, err = ExtractKey(newChild.Cells[0])
		if err != nil {
			return 0, fmt.Errorf("extract key from leaf: %w", err)
		}
		promoteChild = uint32(newChildPage) // ✅ child 是新分裂出来的 leaf 页页号
	} else {
		promoteKey, promoteChild, err = DecodeInternalCell(newChild.Cells[0]) // ✅ child 是 LeftChild
		if err != nil {
			return 0, fmt.Errorf("extract key from internal: %w", err)
		}
	}

	newCell := EncodeInternalCell(promoteKey, promoteChild)

	// 插入 promote cell 到 root
	insertPos := 0
	for i := range root.Cells {
		keyI, _, _ := DecodeInternalCell(root.Cells[i])
		if promoteKey < keyI {
			break
		}
		insertPos++
	}
	root.Cells = append(root.Cells, nil)
	copy(root.Cells[insertPos+1:], root.Cells[insertPos:])
	root.Cells[insertPos] = newCell
	if insertPos == 0 {
		root.LeftChild = promoteChild // ✅ promote 的左边是 promoteChild，不是 newChildPage
	}

	data, err := root.ToBytes()
	if err == nil {
		pager.WritePage(rootPage, data)
		return rootPage, nil
	}

	// ✅ root 自身也满了 → 分裂
	mid := len(root.Cells) / 2
	promoteCell := root.Cells[mid]
	promoteKey, _, err = DecodeInternalCell(promoteCell)
	if err != nil {
		return 0, fmt.Errorf("decode promote key during split: %w", err)
	}

	root.Cells = append(root.Cells[:mid], root.Cells[mid+1:]...)

	left := NewInternalPage()
	right := NewInternalPage()

	left.LeftChild = root.LeftChild
	left.Cells = append(left.Cells, root.Cells[:mid]...)

	right.Cells = append(right.Cells, root.Cells[mid:]...)
	rightPage := pager.AllocatePage()
	right.LeftChild = extractChildFromFirstCell(right)

	pager.WritePage(rootPage, left.ToBytesMust())
	pager.WritePage(rightPage, right.ToBytesMust())

	// ✅ promote key → rightPage（真正 promote 上去）
	newRoot := NewInternalPage()
	newRoot.LeftChild = uint32(rootPage)
	newRoot.Cells = append(newRoot.Cells, EncodeInternalCell(promoteKey, uint32(rightPage)))

	newRootPage := pager.AllocatePage()
	pager.WritePage(newRootPage, newRoot.ToBytesMust())
	return newRootPage, nil
}

func extractChildFromFirstCell(p *Page) uint32 {
	if len(p.Cells) == 0 {
		return 0
	}
	_, child, err := DecodeInternalCell(p.Cells[0])
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

func ExtractPromoteKeyAndChild(pager *Pager, pageNo int) (string, uint32, error) {

	fmt.Println("Use ExtractPromoteKeyAndChild")

	raw, err := pager.ReadPage(pageNo)
	if err != nil {
		return "", 0, err
	}
	page, err := PageFromBytes(raw)
	if err != nil {
		return "", 0, err
	}

	// ✅ InternalPage：提升第一个 cell 的 key，LeftChild 为 child
	if page.Type == PageInternal {
		if len(page.Cells) == 0 {
			return "", 0, fmt.Errorf("empty internal page for promotion")
		}
		key, _, err := DecodeInternalCell(page.Cells[0])
		if err != nil {
			return "", 0, err
		}
		return key, page.LeftChild, nil
	}

	// ✅ LeafPage：提升第一个 record 的 key
	if page.Type == PageLeaf {
		if len(page.Cells) == 0 {
			return "", 0, fmt.Errorf("empty leaf page for promotion")
		}
		key, err := ExtractKey(page.Cells[0])
		if err != nil {
			return "", 0, err
		}
		return key, uint32(pageNo), nil
	}

	return "", 0, fmt.Errorf("unknown page type %d", page.Type)
}
