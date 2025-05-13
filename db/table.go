package db

import (
	"fmt"
	"mySQLite/store"
	"strings"
)

func (db *Database) createTable(sql string) {
	sql = strings.TrimSuffix(sql, ";")
	parts := strings.SplitN(sql, "(", 2)
	head := strings.Fields(parts[0])
	if len(head) < 3 {
		fmt.Println("Invalid CREATE TABLE syntax")
		return
	}
	tableName := head[2]
	cols := strings.TrimSuffix(parts[1], ")")
	colNames := []string{}
	for _, field := range strings.Split(cols, ",") {
		col := strings.Fields(strings.TrimSpace(field))[0]
		colNames = append(colNames, col)
	}

	// 替代错误逻辑：在 CREATE 开始前判断是否已存在
	if _, exists := db.Tables[tableName]; exists {
		fmt.Println("Table already exists, skip CREATE.")
		return
	}

	root := db.Pager.AllocatePage()
	leaf := store.NewLeafPage()
	data, _ := leaf.ToBytes()
	if err := db.Pager.WritePage(root, data); err != nil {
		fmt.Println("Failed to write initial leaf page:", err)
		return
	}

	db.Tables[tableName] = &Table{
		Name:     tableName,
		Columns:  colNames,
		RootPage: root,
		Pager:    db.Pager,
	}

	fmt.Printf("Table created: %s at root page %d\n", tableName, root)

	metaRow := []string{tableName, strings.Join(colNames, "|"), fmt.Sprint(root)}
	data, _ = store.EncodeRow(metaRow)
	err := db.Pager.AppendRow(1, data)
	if err != nil {
		fmt.Println("Error writing table metadata:", err)
	}

}

func (db *Database) insertInto(sql string) {
	sql = strings.TrimSuffix(sql, ";")
	parts := strings.Split(sql, "VALUES")
	if len(parts) != 2 {
		fmt.Println("Invalid INSERT syntax")
		return
	}
	head := strings.Fields(parts[0])
	tableName := head[2]
	values := strings.Trim(strings.TrimSpace(parts[1]), "()")
	valList := strings.Split(values, ",")
	for i := range valList {
		valList[i] = strings.Trim(strings.TrimSpace(valList[i]), "'")
	}
	table, ok := db.Tables[tableName]
	if !ok {
		fmt.Println("Table not found:", tableName)
		return
	}

	encoded, err := store.EncodeRow(valList)
	if err != nil {
		fmt.Println("Error encoding row:", err)
		return
	}

	newRoot, err := store.InsertRow(table.Pager, table.RootPage, encoded) // ✅ 改这里
	if err != nil {
		fmt.Println("Error inserting row:", err)
		return
	}

	if newRoot != table.RootPage {
		oldRoot := table.RootPage
		table.RootPage = newRoot
		fmt.Printf("New root page: %d, old root page: %d\n", newRoot, oldRoot)

		newMeta := []string{table.Name, strings.Join(table.Columns, "|"), fmt.Sprint(newRoot)}
		data, err := store.EncodeRow(newMeta)
		if err != nil {
			fmt.Println("Error encoding table metadata:", err)
			return
		}
		err = db.Pager.UpdateRowInPage(1, table.Name, data)
		if err != nil {
			fmt.Println("Error updating table metadata:", err)
			return
		}
		fmt.Println("Table metadata updated successfully.")
	}

	fmt.Println("Row inserted successfully.")
}

// SEARCH FROM tab WHERE key = '123'
func (db *Database) searchKey(sql string) {
	sql = strings.TrimSuffix(sql, ";")
	tokens := strings.Fields(sql)
	if len(tokens) < 6 || strings.ToUpper(tokens[1]) != "FROM" || strings.ToUpper(tokens[3]) != "WHERE" {
		fmt.Println("Invalid SELECT syntax")
		return
	}
	tableName := tokens[2]
	whereKey := strings.Trim(tokens[6], "'\"")

	fmt.Printf("[DEBUG] Searching key = [%s]\n", whereKey)

	table, ok := db.Tables[tableName]
	if !ok {
		fmt.Println("Table not found:", tableName)
		return
	}

	rowData, err := store.SearchRow(table.Pager, table.RootPage, whereKey)
	if err != nil {
		fmt.Println("Error searching row:", err)
		return
	}
	row, err := store.DecodeRow(rowData)
	if err != nil {
		fmt.Println("Error decoding row:", err)
		return
	}
	fmt.Println(table.Columns)
	fmt.Println(row)
}

// DELETE FROM tab WHERE key = '123'
func (db *Database) deleteFrom(sql string) {
	sql = strings.TrimSuffix(sql, ";")
	tokens := strings.Fields(sql)
	if len(tokens) < 6 || strings.ToUpper(tokens[0]) != "DELETE" || strings.ToUpper(tokens[1]) != "FROM" || strings.ToUpper(tokens[3]) != "WHERE" {
		fmt.Println("Invalid DELETE syntax")
		return
	}
	tableName := tokens[2]
	whereKey := strings.Trim(tokens[6], "'\"")

	fmt.Printf("[DEBUG] Searching key = [%s]\n", whereKey)

	table, ok := db.Tables[tableName]
	if !ok {
		fmt.Println("Table not found:", tableName)
		return
	}
	newRoot, err := store.DeleteRow(table.Pager, table.RootPage, whereKey)
	if err != nil {
		fmt.Println("Error deleting row:", err)
		return
	}

	if newRoot != table.RootPage {
		table.RootPage = newRoot
		meta := []string{table.Name, strings.Join(table.Columns, "|"), fmt.Sprint(newRoot)}
		data, _ := store.EncodeRow(meta)
		db.Pager.UpdateRowInPage(1, table.Name, data)
		fmt.Println("Updated root after delete")
	}

	fmt.Println("Row deleted successfully.")

}
