package store

import "fmt"

func DebugPrintTree(pager *Pager, rootPage int) {
	fmt.Println("=== B+ Tree Structure ===")
	visited := map[int]bool{}
	debugPrintRecursive(pager, rootPage, 0, visited)
}

func debugPrintRecursive(pager *Pager, pageNo int, depth int, visited map[int]bool) {
	if visited[pageNo] {
		fmt.Printf("⚠️ detected loop: page %d already visited\n", pageNo)
		return
	}
	visited[pageNo] = true
	raw, err := pager.ReadPage(pageNo)
	if err != nil {
		fmt.Printf("⚠️ error reading page %d: %v\n", pageNo, err)
		return
	}

	page, err := PageFromBytes(raw)
	if err != nil {
		fmt.Printf("⚠️ error decoding page %d: %v\n", pageNo, err)
		return
	}

	if page.Type == PageLeaf {
		fmt.Printf("%sLeaf Page %d | Cells: %d | NextLeaf: %d\n",
			indent(depth), pageNo, len(page.Cells), page.NextLeaf)
	} else if page.Type == PageInternal {
		fmt.Printf("%sInternal Page %d | Cells: %d | LeftChild: %d\n",
			indent(depth), pageNo, len(page.Cells), page.LeftChild)
		debugPrintRecursive(pager, int(page.LeftChild), depth+1, visited)
		for _, cell := range page.Cells {
			key, child, err := DecodeInternalCell(cell)
			if err != nil {
				fmt.Printf("%s  ⚠️ Failed to decode internal cell\n", indent(depth+1))
				continue
			}
			fmt.Printf("%s  ➤ Key=%s → ChildPage=%d\n", indent(depth+1), key, child)
			debugPrintRecursive(pager, int(child), depth+1, visited)
		}
	} else {
		fmt.Printf("%sUnknown Page Type: %x\n", indent(depth), page.Type)
	}
}

func indent(n int) string {
	return fmt.Sprintf("%s", string(make([]rune, n*2)))
}
