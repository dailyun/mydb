package db

import "mySQLite/store"

func padToPage(data []byte) []byte {
	padded := make([]byte, store.PageSize)
	copy(padded, data)
	return padded
}

func trimPadding(data []byte) []byte {
	end := len(data)
	for end > 0 && data[end-1] == 0 {
		end--
	}
	return data[:end]
}
