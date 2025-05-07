package db

import (
	"fmt"
	"mySQLite/store"
	"strconv"
	"strings"
)

type Row []string

type Table struct {
	Columns  []string
	Name     string
	Pager    *store.Pager
	RootPage int
}

type Database struct {
	Tables map[string]*Table
	Pager  *store.Pager
}

func NewDatabase(pager *store.Pager) *Database {
	tables := make(map[string]*Table)
	rows, _ := pager.ReadAllRows(1)
	for _, r := range rows {
		fields, _ := store.DecodeRow(r)
		if len(fields) != 3 {
			continue
		}
		name := fields[0]
		cols := strings.Split(fields[1], "|")
		root, _ := strconv.Atoi(fields[2])
		tables[name] = &Table{
			Name:     name,
			Columns:  cols,
			RootPage: root,
			Pager:    pager,
		}
	}
	fmt.Println("Recovered tables:")
	for name, t := range tables {
		fmt.Printf("  - %s at root page %d, columns: %v\n", name, t.RootPage, t.Columns)
	}

	return &Database{Tables: tables, Pager: pager}
}
