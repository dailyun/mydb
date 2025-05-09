package test

import (
	"fmt"
	"os"
	"testing"

	"mySQLite/store"
)

func TestInsertSplitAndNewRoot(t *testing.T) {
	os.Remove("test.db")
	pager, _ := store.OpenPager("test.db")
	defer pager.Close()

	rootPage := pager.AllocatePage()

	page := store.NewLeafPage()
	data, _ := page.ToBytes()
	pager.WritePage(rootPage, data)

	for i := 1; i <= 4000; i++ {
		row := []string{fmt.Sprintf("%02d", i), fmt.Sprintf("user%d", i)}
		encoded, _ := store.EncodeRow(row)

		// ✅ 必须更新 rootPage
		var err error
		rootPage, err = store.InsertRow(pager, rootPage, encoded)
		if err != nil {
			t.Errorf("InsertRow failed at i=%d: %v", i, err)
			return
		}

	}
	raw, _ := pager.ReadPage(rootPage)
	root, _ := store.PageFromBytes(raw)
	PrintChildren(pager, root)

	PrintInternalContent(pager, rootPage)

	t.Logf("✅ Final rootPage = %d", rootPage)
}

func PrintInternalContent(pager *store.Pager, pageNo int) {
	raw, _ := pager.ReadPage(pageNo)
	page, _ := store.PageFromBytes(raw)
	if page.Type != store.PageInternal {
		fmt.Printf("Page %d is not Internal\n", pageNo)
		return
	}
	fmt.Printf("📘 Internal Page %d:\n", pageNo)
	fmt.Printf("  LeftChild: %d\n", page.LeftChild)
	for i, cell := range page.Cells {
		key, child, _ := store.DecodeInternalCell(cell)
		fmt.Printf("  [%d] '%s' → Page %d\n", i, key, child)
	}
}
func PrintChildren(pager *store.Pager, page *store.Page) {
	pages := []int{int(page.LeftChild)}
	for _, cell := range page.Cells {
		_, child, _ := store.DecodeInternalCell(cell)
		pages = append(pages, int(child))
	}
	for _, pno := range pages {
		raw, _ := pager.ReadPage(pno)
		childPage, _ := store.PageFromBytes(raw)
		typ := "Unknown"
		if childPage.Type == store.PageLeaf {
			typ = "Leaf"
		} else if childPage.Type == store.PageInternal {
			typ = "Internal"
		}
		fmt.Printf("  ➤ Page %d: %s (%d cells)\n", pno, typ, len(childPage.Cells))
	}
}
