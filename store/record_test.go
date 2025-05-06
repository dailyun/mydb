package store

import (
	"reflect"
	"testing"
)

func TestEncodeDecodeRow(t *testing.T) {
	tests := []struct {
		name string
		row  []string
	}{
		{
			name: "empty row",
			row:  []string{},
		},
		{
			name: "single column",
			row:  []string{"hello"},
		},
		{
			name: "multiple columns",
			row:  []string{"id", "name", "age"},
		},
		{
			name: "unicode and special chars",
			row:  []string{"你好", "😀", "a\nb\tc"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := EncodeRow(tt.row)
			if err != nil {
				t.Fatalf("EncodeRow failed: %v", err)
			}

			decoded, err := DecodeRow(encoded)
			if err != nil {
				t.Fatalf("DecodeRow failed: %v", err)
			}

			if !reflect.DeepEqual(decoded, tt.row) {
				t.Errorf("Decoded row = %v, want %v", decoded, tt.row)
			}
		})
	}
}
