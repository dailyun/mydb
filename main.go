package main

import "mySQLite/db"

func main() {
	mydb := db.NewDatabase()
	mydb.Exec("CREATE TABLE users(id INT, name TEXT);")
	mydb.Exec("INSERT INTO users VALUES(1, 'Alice');")
	mydb.Exec("INSERT INTO users VALUES(2, 'Bob');")
	mydb.Exec("SELECT * FROM users;")

}
