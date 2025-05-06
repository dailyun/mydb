package db

import (
	"fmt"
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
	db.Tables[tableName] = &Table{Columns: colNames}
	fmt.Println("Table created:", tableName)
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
	if table, ok := db.Tables[tableName]; ok {
		table.Rows = append(table.Rows, valList)
		fmt.Println("Inserted into", tableName)
	} else {
		fmt.Println("Table not found:", tableName)
	}
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
	fmt.Println(table.Columns)
	for _, row := range table.Rows {
		fmt.Println(row)
	}
}
