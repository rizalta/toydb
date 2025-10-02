package index

import (
	"fmt"
	"reflect"
	"testing"
)

func TestCursorRange(t *testing.T) {
	idx := newTestIndex(t)
	maxKey := 9999

	t.Log("Inserting keys key_0001 to key_9999")
	fullKeySlice := [][]byte{}
	for i := range maxKey {
		key := fmt.Appendf(nil, "key_%04d", i)
		value := uint64(1000 + i)
		idx.Insert(key, value, Upsert)
		fullKeySlice = append(fullKeySlice, key)
	}

	tests := []struct {
		name     string
		startKey []byte
		endKey   []byte
		expected [][]byte
	}{
		{
			name:     "Full scan",
			startKey: nil,
			endKey:   nil,
			expected: fullKeySlice,
		},
		{
			name:     "Scan with startKey",
			startKey: []byte("key_0055"),
			endKey:   nil,
			expected: fullKeySlice[55:],
		},
		{
			name:     "Scan with endKey",
			startKey: nil,
			endKey:   []byte("key_1551"),
			expected: fullKeySlice[:1551],
		},
		{
			name:     "Bounded scan",
			startKey: []byte("key_0564"),
			endKey:   []byte("key_1709"),
			expected: fullKeySlice[564:1709],
		},
		{
			name:     "endKey out of bounds",
			startKey: []byte("key_0100"),
			endKey:   []byte("key_99999"),
			expected: fullKeySlice[100:],
		},
		{
			name:     "startKey lesser than min key",
			startKey: []byte("key_0"),
			endKey:   []byte("key_1000"),
			expected: fullKeySlice[:1000],
		},
		{
			name:     "startKey greater than maxkey",
			startKey: []byte("key_9999"),
			endKey:   []byte("key_1500"),
			expected: [][]byte{},
		},
		{
			name:     "startKey greater than endKey",
			startKey: []byte("key_5000"),
			endKey:   []byte("key_4999"),
			expected: [][]byte{},
		},
		{
			name:     "Non-existent startKey in the middle",
			startKey: []byte("key_0100a"),
			endKey:   []byte("key_0102"),
			expected: fullKeySlice[101:102],
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := idx.NewCursor(tt.startKey, tt.endKey)
			if err != nil {
				t.Fatalf("failed to initialize cursor at startKey %s, endKey %s: %v", tt.startKey, tt.endKey, err)
			}
			foundKeys := [][]byte{}
			for {
				key, _, err := c.Next()
				if err != nil {
					t.Fatalf("next call failed: %v", err)
				}
				if key == nil {
					break
				}
				foundKeys = append(foundKeys, key)
			}

			if !reflect.DeepEqual(tt.expected, foundKeys) {
				t.Errorf("expected keys not matching with scanned keys for startKey %s, endKey %s", tt.startKey, tt.endKey)
			}
		})
	}
}
