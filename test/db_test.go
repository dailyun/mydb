package test

import (
	"fmt"
	"os"
	"testing"

	"mySQLite/db"
	"mySQLite/store"
)

// 创建干净数据库，返回 db 实例与清理函数
func createTestDB(t *testing.T, filename string) (*db.Database, func()) {
	_ = os.Remove(filename)
	pager, err := store.OpenPager(filename)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	mydb := db.NewDatabase(pager)
	cleanup := func() {
		pager.Close()

	}
	return mydb, cleanup
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

func TestInsertMultipleRows(t *testing.T) {
	db, cleanup := createTestDB(t, "test_insert.db")
	defer cleanup()

	db.Exec("CREATE TABLE users(id, name);")

	for i := 1; i <= 5000; i++ {
		sql := fmt.Sprintf("INSERT INTO users VALUES('%d', 'User%d');", i, i)
		db.Exec(sql)
	}
}

func TestSelectAndCheckRows(t *testing.T) {
	db, cleanup := createTestDB(t, "test_select.db")
	defer cleanup()

	db.Exec("CREATE TABLE users(id, name);")
	total := 50000
	for i := 1; i <= total; i++ {
		sql := fmt.Sprintf("INSERT INTO users VALUES('%d', 'User%d');", i, i)
		db.Exec(sql)
	}

	rows, err := collectRowsFromTree(db.Pager, db.Tables["users"].RootPage)

	for i := 13; i <= total; i += 13 {
		wantID := fmt.Sprintf("%d", i)
		wantName := fmt.Sprintf("User%d", i)
		found := false
		for _, row := range rows {
			if row[0] == wantID && row[1] == wantName {
				found = true
				fmt.Printf("✅ Found row: %v\n", row)
				break
			}
		}
		if !found {
			t.Errorf("❌ Missing row: [%s %s]", wantID, wantName)
		}
	}

	if err != nil {
		t.Fatalf("failed to collect rows: %v", err)
	}

	//for i := 0; i < 3; i++ {
	//	wantID := fmt.Sprintf("%d", i+1)
	//	wantName := fmt.Sprintf("User%d", i+1)
	//	if rows[i][0] != wantID || rows[i][1] != wantName {
	//		t.Errorf("row %d mismatch: got %v, want [%s %s]", i, rows[i], wantID, wantName)
	//	}
	//}
}
