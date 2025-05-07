package main

import (
	"mySQLite/db"
	"mySQLite/store"
)

func main() {
	pager, _ := store.OpenPager("data.db")
	defer pager.Close()
	mydb := db.NewDatabase(pager)
	//mydb.Exec("CREATE TABLE users(id INT, name TEXT);")
	//mydb.Exec("INSERT INTO users VALUES(1, 'Alice');")
	//mydb.Exec("INSERT INTO users VALUES(2, 'Bob');")
	mydb.Exec("SELECT * FROM users;")

}
