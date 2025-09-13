package index

import (
	"errors"
	"math/rand"
	"testing"
)

func TestDeleteBasicScenarios(t *testing.T) {
	m := map[uint64]uint64{10: 100, 20: 200, 30: 300, 40: 400}

	tests := []struct {
		name      string
		deleteKey uint64
		expected  []uint64
	}{
		{
			name:      "Verify_delete_existing_key",
			deleteKey: 20,
			expected:  []uint64{10, 30, 40},
		},
		{
			name:      "Verify_delete_smallest_key",
			deleteKey: 10,
			expected:  []uint64{20, 30, 40},
		},
		{
			name:      "Verify_delete_largest_key",
			deleteKey: 40,
			expected:  []uint64{10, 20, 30},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index := newTestIndex(t)
			defer index.Close()

			for key, value := range m {
				if err := index.Insert(key, value, Upsert); err != nil {
					t.Fatalf("failed to insert key %d into index: %v", key, err)
				}
			}

			t.Run("Verify_key_is_deleted", func(t *testing.T) {
				err := index.Delete(tt.deleteKey)
				if err != nil {
					t.Fatalf("failed to delete key %d: %v", tt.deleteKey, err)
				}
				_, err = index.Search(tt.deleteKey)
				if !errors.Is(err, ErrKeyNotFound) {
					t.Errorf("expected %v for err, got %v", ErrKeyNotFound, err)
				}
			})

			t.Run("Verify_all_other_keys", func(t *testing.T) {
				for _, key := range tt.expected {
					value, err := index.Search(key)
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

	m := map[uint64]uint64{10: 100, 20: 200, 30: 300, 40: 400}

	for key, value := range m {
		if err := index.Insert(key, value, Upsert); err != nil {
			t.Fatalf("failed to insert key %d into index: %v", key, err)
		}
	}

	t.Run("Verify_delete_invalid_key", func(t *testing.T) {
		deleteKey := uint64(88)
		err := index.Delete(deleteKey)
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("expected error as %v, got %v when deleting invalid key", ErrKeyNotFound, err)
		}
	})

	t.Run("Verify_all_other_keys", func(t *testing.T) {
		expected := []uint64{10, 20, 30, 40}
		for _, key := range expected {
			value, err := index.Search(key)
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

	err := index.Delete(10)
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected error %v when deleting empty index, got %v", ErrKeyNotFound, err)
	}
}

func TestDeleteKeyTwice(t *testing.T) {
	index := newTestIndex(t)
	defer index.Close()

	m := map[uint64]uint64{10: 100, 20: 200, 30: 300, 40: 400}

	for key, value := range m {
		if err := index.Insert(key, value, Upsert); err != nil {
			t.Fatalf("failed to insert key %d into index: %v", key, err)
		}
	}

	t.Run("Verify_delete_key_twice", func(t *testing.T) {
		deleteKey := uint64(20)
		err := index.Delete(deleteKey)
		if err != nil {
			t.Fatalf("failed to delete key first time for key %d: %v", deleteKey, err)
		}
		err = index.Delete(deleteKey)
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("expected error as %v, got %v when deleting key twice", ErrKeyNotFound, err)
		}
	})

	t.Run("Verify_all_other_keys", func(t *testing.T) {
		expected := []uint64{10, 30, 40}
		for _, key := range expected {
			value, err := index.Search(key)
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

	key := uint64(10)
	value := uint64(100)
	err := index.Insert(key, value, Upsert)
	if err != nil {
		t.Fatalf("failed to insert key %d into index: %v", key, err)
	}

	err = index.Delete(key)
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

	m := make(map[uint64]uint64)
	for i := range MaxKeys + 1 {
		m[uint64(i)] = uint64(i + 1000)
	}

	for key, value := range m {
		err := index.Insert(key, value, Upsert)
		if err != nil {
			t.Fatalf("failed to insert for key %d: %v", key, err)
		}
	}

	deleteKey := uint64(127)
	err := index.Delete(deleteKey)
	if err != nil {
		t.Fatalf("failed to delete key %d: %v", deleteKey, err)
	}
	delete(m, deleteKey)

	t.Run("Verify_delete_underflow", func(t *testing.T) {
		deleteKey := uint64(126)
		err = index.Delete(deleteKey)
		if err != nil {
			t.Fatalf("failed to delete key %d: %v", deleteKey, err)
		}
		delete(m, deleteKey)

		_, err := index.Search(deleteKey)
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("expected error %v, got %v when deleting for underflow", ErrKeyNotFound, err)
		}
	})

	t.Run("Verify_all_other_keys", func(t *testing.T) {
		for key, expectedValue := range m {
			value, err := index.Search(key)
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

	m := make(map[uint64]uint64)
	for i := range MaxKeys + 1 {
		m[uint64(i)] = uint64(i + 1000)
	}

	for key, value := range m {
		err := index.Insert(key, value, Upsert)
		if err != nil {
			t.Fatalf("failed to insert for key %d: %v", key, err)
		}
	}

	deleteKey := uint64(127)
	err := index.Delete(deleteKey)
	if err != nil {
		t.Fatalf("failed to delete key %d: %v", deleteKey, err)
	}
	delete(m, deleteKey)

	deleteKey = uint64(255)
	err = index.Delete(deleteKey)
	if err != nil {
		t.Fatalf("failed to delete key %d: %v", deleteKey, err)
	}
	delete(m, deleteKey)

	t.Run("Verify_delete_underflow", func(t *testing.T) {
		deleteKey := uint64(126)
		err = index.Delete(deleteKey)
		if err != nil {
			t.Fatalf("failed to delete key %d: %v", deleteKey, err)
		}
		delete(m, deleteKey)

		_, err := index.Search(deleteKey)
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("expected error %v, got %v when deleting for underflow", ErrKeyNotFound, err)
		}
	})

	t.Run("Verify_all_other_keys", func(t *testing.T) {
		for key, expectedValue := range m {
			value, err := index.Search(key)
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

	inserts := make(map[uint64]uint64)
	deletes := make(map[uint64]struct{})

	seed := 42
	r := rand.New(rand.NewSource(int64(seed)))

	isInsert := func() bool { return r.Intn(2) == 0 }

	numOperations := 20000
	maxKey := 5000

	for range numOperations {
		key := uint64(r.Intn(maxKey))
		value := uint64(r.Intn(maxKey))

		if isInsert() {
			err := index.Insert(key, value, Upsert)
			if err != nil {
				t.Fatalf("failed to insert key %d: %v", key, err)
			}
			inserts[key] = value
			delete(deletes, key)

		} else {
			err := index.Delete(key)
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
			_, err := index.Search(key)
			if !errors.Is(err, ErrKeyNotFound) {
				t.Errorf("expected error %v, got %v for key %d", ErrKeyNotFound, err, key)
			}
		}
	})

	t.Run("Verify_all_other_keys", func(t *testing.T) {
		for key, expectedValue := range inserts {
			value, err := index.Search(key)
			if err != nil {
				t.Fatalf("failed to search for key %d: %v", key, err)
			}
			if value != expectedValue {
				t.Errorf("expected value %d for key %d, got %d", expectedValue, key, value)
			}
		}
	})
}
