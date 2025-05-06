package test

import (
	"mySQLite/db"
	"testing"
)

func TestInsertAndSelect(t *testing.T) {
	d := db.NewDatabase()
	d.Exec("CREATE TABLE test(id INT, name TEXT);")
	d.Exec("INSERT INTO test VALUES(1, 'Test');")
	d.Exec("SELECT * FROM test;")
}
