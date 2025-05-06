package db

import "mySQLite/store"

type Row []string

type Table struct {
	Columns  []string
	Rows     []Row
	Pager    *store.Pager
	RootPage int
}

type Database struct {
	Tables map[string]*Table
}

func NewDatabase() *Database {
	return &Database{Tables: make(map[string]*Table)}
}
