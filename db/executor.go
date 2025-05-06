package db

import (
	"fmt"
	"strings"
)

func (db *Database) Exec(sql string) {
	tokens := strings.Fields(sql)
	if len(tokens) == 0 {
		fmt.Println("Empty SQL")
		return
	}
	switch strings.ToUpper(tokens[0]) {
	case "CREATE":
		db.createTable(sql)
	case "INSERT":
		db.insertInto(sql)
	case "SELECT":
		db.selectFrom(sql)
	default:
		fmt.Println("Unsupported SQL:", sql)
	}
}
