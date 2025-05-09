package store

import (
	"sort"
	"strconv"
)

func sortInternalCells(p *Page) {
	sort.Slice(p.Cells, func(i, j int) bool {
		keyI, _, _ := DecodeInternalCell(p.Cells[i])
		keyJ, _, _ := DecodeInternalCell(p.Cells[j])
		intI, _ := strconv.Atoi(keyI)
		intJ, _ := strconv.Atoi(keyJ)
		return intI < intJ
	})
}
