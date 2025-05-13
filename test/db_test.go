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
	testDB, cleanup := createTestDB(t, "test_select.testDB")
	defer cleanup()

	testDB.Exec("CREATE TABLE users(id, name, gender);")
	total := 1500
	for i := 1; i <= total; i++ {
		sql := fmt.Sprintf("INSERT INTO users VALUES('%d', 'User%d', 1);", i, i)
		testDB.Exec(sql)
	}

	rows, err := collectRowsFromTree(testDB.Pager, testDB.Tables["users"].RootPage)
	store.DebugPrintTree(testDB.Pager, testDB.Tables["users"].RootPage)

	fmt.Println(len(rows))

	if err != nil {
		t.Fatalf("failed to collect rows: %v", err)
	}

}

func TestDebugPrintTree(t *testing.T) {
	db, cleanup := createTestDB(t, "test_debug.db")
	defer cleanup()

	db.Exec("CREATE TABLE users (id, name);")
	for i := 0; i < 150; i++ {
		row := fmt.Sprintf("INSERT INTO users VALUES ('%d', 'user%d');", i, i)
		db.Exec(row)
	}
	db.Exec("SEARCH FROM users WHERE Key = '1';")
	db.Exec("DELETE FROM users WHERE key = '1';")

	//fmt.Println("\n=== B+ Tree Visual Structure ===")
	//store.DebugPrintTree(db.Pager, db.Tables["users"].RootPage)
}
