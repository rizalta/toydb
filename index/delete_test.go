package index

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

func makeKey(i int) []byte {
	return fmt.Appendf(nil, "key_%010d", i)
}

func TestDeleteBasicScenarios(t *testing.T) {
	m := map[int]uint64{10: 100, 20: 200, 30: 300, 40: 400}

	tests := []struct {
		name      string
		deleteKey int
		expected  []int
	}{
		{
			name:      "Verify_delete_existing_key",
			deleteKey: 20,
			expected:  []int{10, 30, 40},
		},
		{
			name:      "Verify_delete_smallest_key",
			deleteKey: 10,
			expected:  []int{20, 30, 40},
		},
		{
			name:      "Verify_delete_largest_key",
			deleteKey: 40,
			expected:  []int{10, 20, 30},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index := newTestIndex(t)
			defer index.Close()

			for key, value := range m {
				if err := index.Insert(makeKey(key), value, Upsert); err != nil {
					t.Fatalf("failed to insert key %d into index: %v", key, err)
				}
			}

			t.Run("Verify_key_is_deleted", func(t *testing.T) {
				err := index.Delete(makeKey(tt.deleteKey))
				if err != nil {
					t.Fatalf("failed to delete key %d: %v", tt.deleteKey, err)
				}
				_, err = index.Search(makeKey(tt.deleteKey))
				if !errors.Is(err, ErrKeyNotFound) {
					t.Errorf("expected %v for err, got %v", ErrKeyNotFound, err)
				}
			})

			t.Run("Verify_all_other_keys", func(t *testing.T) {
				for _, key := range tt.expected {
					value, err := index.Search(makeKey(key))
					if err != nil {
						t.Fatalf("failed to search for key %d: %v", key, err)
					}
					if value != m[key] {
						t.Errorf("expected value %d for key %d, got %d", m[key], key, value)
					}
				}
			})
		})
	}
}

func TestDeleteInvalidKey(t *testing.T) {
	index := newTestIndex(t)
	defer index.Close()

	m := map[int]uint64{10: 100, 20: 200, 30: 300, 40: 400}

	for key, value := range m {
		if err := index.Insert(makeKey(key), value, Upsert); err != nil {
			t.Fatalf("failed to insert key %d into index: %v", key, err)
		}
	}

	t.Run("Verify_delete_invalid_key", func(t *testing.T) {
		deleteKey := 88
		err := index.Delete(makeKey(deleteKey))
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("expected error as %v, got %v when deleting invalid key", ErrKeyNotFound, err)
		}
	})

	t.Run("Verify_all_other_keys", func(t *testing.T) {
		expected := []int{10, 20, 30, 40}
		for _, key := range expected {
			value, err := index.Search(makeKey(key))
			if err != nil {
				t.Fatalf("failed to search for key %d: %v", key, err)
			}
			if value != m[key] {
				t.Errorf("expected value %d for key %d, got %d", m[key], key, value)
			}
		}
	})
}

func TestDeleteEmptyIndex(t *testing.T) {
	index := newTestIndex(t)
	defer index.Close()

	err := index.Delete(makeKey(10))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected error %v when deleting empty index, got %v", ErrKeyNotFound, err)
	}
}

func TestDeleteKeyTwice(t *testing.T) {
	index := newTestIndex(t)
	defer index.Close()

	m := map[int]uint64{10: 100, 20: 200, 30: 300, 40: 400}

	for key, value := range m {
		if err := index.Insert(makeKey(key), value, Upsert); err != nil {
			t.Fatalf("failed to insert key %d into index: %v", key, err)
		}
	}

	t.Run("Verify_delete_key_twice", func(t *testing.T) {
		deleteKey := 20
		err := index.Delete(makeKey(deleteKey))
		if err != nil {
			t.Fatalf("failed to delete key first time for key %d: %v", deleteKey, err)
		}
		err = index.Delete(makeKey(deleteKey))
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("expected error as %v, got %v when deleting key twice", ErrKeyNotFound, err)
		}
	})

	t.Run("Verify_all_other_keys", func(t *testing.T) {
		expected := []int{10, 30, 40}
		for _, key := range expected {
			value, err := index.Search(makeKey(key))
			if err != nil {
				t.Fatalf("failed to search for key %d: %v", key, err)
			}
			if value != m[key] {
				t.Errorf("expected value %d for key %d, got %d", m[key], key, value)
			}
		}
	})
}

func TestDeleteOnlyKey(t *testing.T) {
	index := newTestIndex(t)
	defer index.Close()

	key := 10
	value := uint64(100)
	err := index.Insert(makeKey(key), value, Upsert)
	if err != nil {
		t.Fatalf("failed to insert key %d into index: %v", key, err)
	}

	err = index.Delete(makeKey(key))
	if err != nil {
		t.Fatalf("failed to delete the only key %d: %v", key, err)
	}

	rootNode, _, _ := index.readNode(index.root)
	if len(rootNode.keys) != 0 {
		t.Errorf("expected 0 keys for root node, got %d", len(rootNode.keys))
	}
}

func TestDeleteUnderflowBorrow(t *testing.T) {
	index := newTestIndex(t)

	m := make(map[int]uint64)
	i := 0
	for {
		key := makeKey(i)
		value := uint64(i + 1000)
		rootNode, _, _ := index.readNode(index.root)
		entrySize := len(key) + slotSize + valueSize
		if rootNode.calculateSize()+entrySize > splitThreshold {
			break
		}
		m[i] = value
		err := index.Insert(key, value, Upsert)
		if err != nil {
			t.Fatalf("failed to insert for key %d: %v", i, err)
		}
		i++
	}
	err := index.Insert(makeKey(i), uint64(i+1000), Upsert)
	if err != nil {
		t.Fatalf("failed to insert for key %d: %v", i, err)
	}

	rootNode, _, _ := index.readNode(index.root)
	leftChildID := rootNode.children[0]
	leftChild, _, _ := index.readNode(leftChildID)
	deleteKey := leftChild.keys[len(leftChild.keys)-1]
	deleteKeyNum, _ := strconv.Atoi(string(deleteKey)[4:])
	err = index.Delete(deleteKey)
	if err != nil {
		t.Fatalf("failed to delete key %d: %v", deleteKeyNum, err)
	}
	delete(m, deleteKeyNum)

	t.Run("Verify_delete_underflow", func(t *testing.T) {
		deleteKey := leftChild.keys[len(leftChild.keys)-2]
		deleteKeyNum, _ := strconv.Atoi(string(deleteKey)[4:])
		err = index.Delete(deleteKey)
		if err != nil {
			t.Fatalf("failed to delete key %d: %v", deleteKeyNum, err)
		}
		delete(m, deleteKeyNum)

		_, err := index.Search(deleteKey)
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("expected error %v, got %v when deleting for underflow", ErrKeyNotFound, err)
		}
	})

	t.Run("Verify_all_other_keys", func(t *testing.T) {
		for key, expectedValue := range m {
			value, err := index.Search(makeKey(key))
			if err != nil {
				t.Fatalf("failed to search for key %d: %v", key, err)
			}
			if value != expectedValue {
				t.Errorf("expected value %d for key %d, got %d", expectedValue, key, value)
			}
		}
	})
}

func TestDeleteUnderflowMerge(t *testing.T) {
	index := newTestIndex(t)

	m := make(map[int]uint64)
	i := 0
	for {
		rootNode, _, _ := index.readNode(index.root)
		key := makeKey(i)
		value := uint64(i + 1000)
		entrySize := len(key) + slotSize + valueSize
		if rootNode.calculateSize()+entrySize > splitThreshold {
			break
		}
		m[i] = value
		err := index.Insert(key, value, Upsert)
		if err != nil {
			t.Fatalf("failed to insert for key %d: %v", key, err)
		}
		i++
	}
	err := index.Insert(makeKey(i), uint64(i+1000), Upsert)
	if err != nil {
		t.Fatalf("failed to insert for key %d: %v", i, err)
	}

	rootNode, _, _ := index.readNode(index.root)
	leftChildID, rightChildID := rootNode.children[0], rootNode.children[1]
	leftChild, _, _ := index.readNode(leftChildID)
	rightChild, _, _ := index.readNode(rightChildID)
	deleteKey := leftChild.keys[len(leftChild.keys)-1]
	deleteKeyNum, _ := strconv.Atoi(string(deleteKey)[4:])
	err = index.Delete(deleteKey)
	if err != nil {
		t.Fatalf("failed to delete key %d: %v", deleteKeyNum, err)
	}
	delete(m, deleteKeyNum)

	deleteKey = rightChild.keys[len(rightChild.keys)-1]
	deleteKeyNum, _ = strconv.Atoi(string(deleteKey)[4:])
	err = index.Delete(deleteKey)
	if err != nil {
		t.Fatalf("failed to delete key %d: %v", deleteKeyNum, err)
	}
	delete(m, deleteKeyNum)

	t.Run("Verify_delete_underflow", func(t *testing.T) {
		deleteKey := leftChild.keys[len(leftChild.keys)-2]
		deleteKeyNum, _ = strconv.Atoi(string(deleteKey)[4:])
		err = index.Delete(deleteKey)
		if err != nil {
			t.Fatalf("failed to delete key %d: %v", deleteKeyNum, err)
		}
		delete(m, deleteKeyNum)

		_, err := index.Search(deleteKey)
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("expected error %v, got %v when deleting for underflow", ErrKeyNotFound, err)
		}
	})

	t.Run("Verify_all_other_keys", func(t *testing.T) {
		for key, expectedValue := range m {
			value, err := index.Search(makeKey(key))
			if err != nil {
				t.Fatalf("failed to search for key %d: %v", key, err)
			}
			if value != expectedValue {
				t.Errorf("expected value %d for key %d, got %d", expectedValue, key, value)
			}
		}
	})
}

func TestDeleteStress(t *testing.T) {
	index := newTestIndex(t)

	inserts := make(map[int]uint64)
	deletes := make(map[int]struct{})

	seed := 42
	r := rand.New(rand.NewSource(int64(seed)))

	isInsert := func() bool { return r.Intn(2) == 0 }
	makeKey := func(i int) []byte {
		padding := strings.Repeat("0", i%10)
		return fmt.Appendf(nil, "key_%s%d", padding, i)
	}

	numOperations := 20000
	maxKey := 5000

	for range numOperations {
		key := r.Intn(maxKey)
		value := uint64(r.Intn(maxKey))

		if isInsert() {
			err := index.Insert(makeKey(key), value, Upsert)
			if err != nil {
				t.Fatalf("failed to insert key %d: %v", key, err)
			}
			inserts[key] = value
			delete(deletes, key)

		} else {
			err := index.Delete(makeKey(key))
			if _, exist := inserts[key]; exist {
				if err != nil {
					t.Fatalf("failed to delete key %d: %v", key, err)
				}
				delete(inserts, key)
				deletes[key] = struct{}{}
			} else {
				if !errors.Is(err, ErrKeyNotFound) {
					t.Errorf("expected error %v, got %v when deleting key %d", ErrKeyNotFound, err, key)
				}
			}
		}
	}

	t.Run("Verify_deleted_keys_not_found", func(t *testing.T) {
		for key := range deletes {
			_, err := index.Search(makeKey(key))
			if !errors.Is(err, ErrKeyNotFound) {
				t.Errorf("expected error %v, got %v for key %d", ErrKeyNotFound, err, key)
			}
		}
	})

	t.Run("Verify_all_other_keys", func(t *testing.T) {
		for key, expectedValue := range inserts {
			value, err := index.Search(makeKey(key))
			if err != nil {
				t.Fatalf("failed to search for key %d: %v", key, err)
			}
			if value != expectedValue {
				t.Errorf("expected value %d for key %d, got %d", expectedValue, key, value)
			}
		}
	})
}
