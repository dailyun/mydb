package db

import (
	"bytes"
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
	db.Tables[tableName] = &Table{
		Name:     tableName,
		Columns:  colNames,
		RootPage: root,
		Pager:    db.Pager,
	}

	fmt.Printf("Table created: %s at root page %d\n", tableName, root)

	metaRow := []string{tableName, strings.Join(colNames, "|"), fmt.Sprint(root)}
	data, _ := store.EncodeRow(metaRow)
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

	encoded, _ := store.EncodeRow(valList)
	rawPage, err := table.Pager.ReadPage(table.RootPage)
	if err != nil || bytes.Equal(rawPage, make([]byte, store.PageSize)) {
		page := store.NewLeafPage()
		page.InsertCell(encoded)
		data, _ := page.ToBytes()
		err := table.Pager.WritePage(table.RootPage, data)
		if err != nil {
			return
		}
		fmt.Println("Inserted into table:", tableName)
		return
	}

	page, err := store.PageFromBytes(rawPage)
	if err != nil {
		fmt.Println("Failed to parse B-tree page:", err)
		return
	}

	if err := page.InsertCell(encoded); err != nil {
		fmt.Println("Error inserting into table:", err)
		return
	}
	data, _ := page.ToBytes()
	table.Pager.WritePage(table.RootPage, data)
	fmt.Println("Inserted into table:", tableName)
}

func (db *Database) selectFrom(sql string) {
	sql = strings.TrimSuffix(sql, ";")
	tokens := strings.Fields(sql)
	if len(tokens) < 4 || strings.ToUpper(tokens[1]) != "*" || strings.ToUpper(tokens[2]) != "FROM" {
		fmt.Println("Invalid SELECT syntax")
		return
	}
	tableName := tokens[3]
	table, ok := db.Tables[tableName]
	if !ok {
		fmt.Println("Table not found:", tableName)
		return
	}
	rawPage, err := table.Pager.ReadPage(table.RootPage)
	if err != nil {
		fmt.Println("Read error:", err)
		return
	}

	page, err := store.PageFromBytes(rawPage)
	if err != nil {
		fmt.Println("Failed to parse B-tree page:", err)
		return
	}

	fmt.Println(table.Columns)
	for _, cell := range page.Cells {
		fmt.Printf("Raw cell length: %d\n", len(cell))
		row, err := store.DecodeRow(cell)
		if err != nil {
			fmt.Println("Error decoding row:", err)
			continue
		}
		fmt.Println(row)
	}

}
