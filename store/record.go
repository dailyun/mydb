package store

import (
	"bytes"
	"encoding/binary"
	"io"
)

func EncodeRow(row []string) ([]byte, error) {
	buf := new(bytes.Buffer)
	//  写入字段数
	binary.Write(buf, binary.LittleEndian, int32(len(row)))
	for _, field := range row {
		data := []byte(field)
		binary.Write(buf, binary.LittleEndian, int32(len(data)))
		buf.Write(data)
	}
	return buf.Bytes(), nil
}

// 解码行
func DecodeRow(data []byte) ([]string, error) {
	buf := bytes.NewBuffer(data)
	var count int32
	if err := binary.Read(buf, binary.LittleEndian, &count); err != nil {
		return nil, err
	}
	row := make([]string, count)
	for i := 0; i < int(count); i++ {
		var size int32
		if err := binary.Read(buf, binary.LittleEndian, &size); err != nil {
			return nil, err
		}
		field := make([]byte, size)
		if _, err := io.ReadFull(buf, field); err != nil {
			return nil, err
		}
		row[i] = string(field)
	}
	return row, nil
}
