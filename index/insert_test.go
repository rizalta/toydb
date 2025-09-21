package index

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestInsertSimple(t *testing.T) {
	index := newTestIndex(t)
	defer index.Close()
	if index == nil {
		t.Fatalf("expected index, got nil")
	}

	t.Run("Insert_one_key_value", func(t *testing.T) {
		key1 := []byte("key1")
		err := index.Insert(key1, 100, Upsert)
		if err != nil {
			t.Fatalf("failed to insert key %s into index", key1)
		}
		val, err := index.Search(key1)
		if err != nil {
			t.Fatalf("search for key %s should not give err: %v", key1, err)
		}
		if val != 100 {
			t.Errorf("expected value 100 for key %s, got %d", key1, val)
		}
	})

	t.Run("Insert_second_key_value", func(t *testing.T) {
		key2 := []byte("key2")
		err := index.Insert(key2, 50, Upsert)
		if err != nil {
			t.Fatalf("failed to insert key %s into index", key2)
		}
		val, err := index.Search(key2)
		if err != nil {
			t.Fatalf("search for key %s should not give err: %v", key2, err)
		}
		if val != 50 {
			t.Errorf("expected value 50 for key %s, got %d", key2, val)
		}
		val, err = index.Search([]byte("key1"))
		if err != nil {
			t.Fatalf("search for key key1 should not give err: %v", err)
		}
		if val != 100 {
			t.Errorf("expected value 100 for key key1, got %d", val)
		}
	})

	t.Run("Validate_keys_are_sorted_in_page", func(t *testing.T) {
		root, _, _ := index.readNode(1)
		expectedKeys := [][]byte{[]byte("key1"), []byte("key2")}
		if !reflect.DeepEqual(root.keys, expectedKeys) {
			t.Errorf("expected keys in root node as %v, got %v", expectedKeys, root.keys)
		}
	})
}

func TestInsertModes(t *testing.T) {
	index := newTestIndex(t)
	defer index.Close()

	key := []byte("100")
	value1 := uint64(1000)
	value2 := uint64(2000)

	t.Run("UpdateOnly_non_existant", func(t *testing.T) {
		err := index.Insert(key, value1, UpdateOnly)
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("expected error %v, got %v", ErrKeyNotFound, err)
		}
	})

	t.Run("InsertOnly_initial", func(t *testing.T) {
		err := index.Insert(key, value1, InsertOnly)
		if err != nil {
			t.Fatalf("initial insert failed: %v", err)
		}
	})

	t.Run("InsertOnly_duplicate", func(t *testing.T) {
		err := index.Insert(key, value2, InsertOnly)
		if !errors.Is(err, ErrKeyAlreadyExists) {
			t.Errorf("expected error %v, got %v", ErrKeyAlreadyExists, err)
		}
	})

	t.Run("UpdateOnly_existing", func(t *testing.T) {
		err := index.Insert(key, value2, UpdateOnly)
		if err != nil {
			t.Fatalf("update only on existing key failed: %v", err)
		}

		val, err := index.Search(key)
		if err != nil {
			t.Fatalf("search failed after update only: %v", err)
		}
		if val != value2 {
			t.Errorf("expected value %v, got %v after udpate only", value2, val)
		}
	})

	t.Run("Upsert_existing", func(t *testing.T) {
		err := index.Insert(key, value1, Upsert)
		if err != nil {
			t.Fatalf("upsert on existing key failed: %v", err)
		}

		val, err := index.Search(key)
		if err != nil {
			t.Fatalf("search failed after upsert: %v", err)
		}
		if val != value1 {
			t.Errorf("expected value %v, got %v after upsert", value1, val)
		}
	})
}

func TestInsertAndSplit(t *testing.T) {
	index := newTestIndex(t)
	defer index.Close()
	if index == nil {
		t.Fatalf("expected index, got nil")
	}

	i := 0
	for {
		key := fmt.Appendf(nil, "key_%04d", i)
		value := uint64(1000 + i)
		rootNode, _, _ := index.readNode(index.root)
		keySize := len(key)
		entrySize := keySize + slotSize + valueSize
		err := index.Insert(key, value, Upsert)
		if rootNode.calculateSize()+entrySize > splitThreshold {
			break
		}
		if err != nil {
			t.Fatalf("failed to insert (%d, %d) into index: %v", key, value, err)
		}
		i++
	}

	splitKey := fmt.Appendf(nil, "key_%04d", i)
	splitValue := uint64(1000 + i)

	err := index.Insert(splitKey, splitValue, Upsert)
	if err != nil {
		t.Fatalf("failed to insert (%d, %d) into index which should trigger split: %v", splitKey, splitValue, err)
	}

	root, _, err := index.readNode(index.root)
	if err != nil {
		t.Fatalf("failed to read root node from page id %d: %v", index.root, err)
	}

	t.Run("Verify_root_node", func(t *testing.T) {
		if root.nodeType != NodeTypeInternal {
			t.Errorf("expected root nodeType to be NodeTypeInternal, got %v", root.nodeType)
		}
		if len(root.keys) != 1 {
			t.Errorf("expected num of keys for root node be 1, got %d", len(root.keys))
		}
		if len(root.children) != 2 {
			t.Errorf("expected num of children for root node be 2, got %d", len(root.children))
		}
	})

	leftChildID, rightChildID := root.children[0], root.children[1]
	leftChild, _, err := index.readNode(leftChildID)
	if err != nil {
		t.Fatalf("failed to read left child: %v", err)
	}
	rightChild, _, err := index.readNode(rightChildID)
	if err != nil {
		t.Fatalf("failed to read right child: %v", err)
	}

	t.Run("Verify_left_child", func(t *testing.T) {
		if leftChild.nodeType != NodeTypeLeaf {
			t.Errorf("expected left child nodeType to be NodeTypeInternal, got %v", leftChild.nodeType)
		}

		if leftChild.calculateSize() < mergeThreshold {
			t.Errorf("expected left child size to be greater than %v, got %v", mergeThreshold, leftChild.calculateSize())
		}

		if leftChild.next != rightChildID {
			t.Errorf("expected left child next to be %v, got %v", rightChildID, leftChild.next)
		}
	})

	t.Run("Verify_right_child", func(t *testing.T) {
		if rightChild.nodeType != NodeTypeLeaf {
			t.Errorf("expected right child nodeType to be NodeTypeInternal, got %v", rightChild.nodeType)
		}

		if rightChild.calculateSize() < mergeThreshold {
			t.Errorf("expected right child size to be greater than %v, got %v", mergeThreshold, rightChild.calculateSize())
		}
	})

	t.Run("Verify_all_keys", func(t *testing.T) {
		totalKeysInserted := i + 1
		for i := range totalKeysInserted {
			key := fmt.Appendf(nil, "key_%04d", i)
			value := uint64(1000 + i)

			val, err := index.Search(key)
			if err != nil {
				t.Fatalf("failed to search key %s: %v", key, err)
			}
			if value != val {
				t.Errorf("expected value %d for key %s, got %d", value, key, val)
			}
		}
	})
}

func TestInternalNodeSplit(t *testing.T) {
	index := newTestIndex(t)
	defer index.Close()
	if index == nil {
		t.Fatalf("expected index, got nil")
	}

	var keyCounter uint64 = 0
	makeKey := func() []byte {
		return fmt.Appendf(nil, "key_%010d", keyCounter)
	}
	value := func() uint64 { return keyCounter + 1000 }

	t.Log("phase1: Creating initial internal root")
	for {
		key := makeKey()
		rootNode, _, _ := index.readNode(index.root)
		entrySize := len(key) + slotSize + valueSize
		if rootNode.calculateSize()+entrySize > splitThreshold {
			break
		}
		err := index.Insert(key, value(), Upsert)
		if err != nil {
			t.Fatalf("phase1: failed to insert key %s, : %v", key, err)
		}
		keyCounter++
	}
	err := index.Insert(makeKey(), value(), Upsert)
	if err != nil {
		t.Fatalf("phase1: split failed: %v", err)
	}
	keyCounter++

	t.Log("phase2: Filling internal root node")
	for {
		rootNode, _, _ := index.readNode(index.root)
		key := makeKey()
		if rootNode.calculateSize()+len(key)+slotSize+childSize > splitThreshold {
			break
		}
		for {
			rightChildID := rootNode.children[len(rootNode.children)-1]
			rightChild, _, _ := index.readNode(rightChildID)
			key := makeKey()
			entrySize := len(key) + slotSize + valueSize
			if rightChild.calculateSize()+entrySize > splitThreshold {
				break
			}
			err := index.Insert(key, value(), Upsert)
			if err != nil {
				t.Fatalf("phase2: failed to insert key %s: %v", key, err)
			}
			keyCounter++
		}
		err := index.Insert(makeKey(), value(), Upsert)
		if err != nil {
			t.Fatalf("phase2: rightmost child split failed: %v", err)
		}
		keyCounter++
	}

	t.Log("phase3: trigger internal node split")
	rootNode, _, _ := index.readNode(index.root)
	for {
		rightChildID := rootNode.children[len(rootNode.children)-1]
		rightChild, _, _ := index.readNode(rightChildID)
		key := makeKey()
		entrySize := len(key) + slotSize + valueSize
		if rightChild.calculateSize()+entrySize > splitThreshold {
			break
		}
		err := index.Insert(key, value(), Upsert)
		if err != nil {
			t.Fatalf("phase3: failed to insert key %s, : %v", key, err)
		}
		keyCounter++
	}

	finalKeyCount := keyCounter
	err = index.Insert(makeKey(), value(), Upsert)
	if err != nil {
		t.Fatalf("phase3: failed to insert trigger key %s: %v", makeKey(), err)
	}

	t.Log("phase4: verifying tree structure and data integrity")

	t.Run("Verify_new_root", func(t *testing.T) {
		newRoot, _, err := index.readNode(index.root)
		if err != nil {
			t.Fatalf("failed to read new root: %v", err)
		}
		if newRoot.nodeType != NodeTypeInternal {
			t.Fatalf("expected new root to be internal, got %v", newRoot.nodeType)
		}
		if len(newRoot.keys) != 1 {
			t.Fatalf("expected new root to have 1 key, got %d", len(newRoot.keys))
		}
		if len(newRoot.children) != 2 {
			t.Errorf("expected new root to have 2 children, got %d", len(newRoot.children))
		}
	})

	t.Run("Verify_Tree_height_increased", func(t *testing.T) {
		newRoot, _, err := index.readNode(index.root)
		if err != nil {
			t.Fatalf("failed to read new root: %v", err)
		}
		child, _, err := index.readNode(newRoot.children[0])
		if err != nil {
			t.Fatalf("failed to read child of new root: %v", err)
		}
		if child.nodeType != NodeTypeInternal {
			t.Errorf("expected child of new root to be internal (tree height 3), but got %v", child.nodeType)
		}
	})

	t.Run("Verify_all_keys_searchable", func(t *testing.T) {
		keyCounter = 0
		for keyCounter <= finalKeyCount {
			key := makeKey()
			val, err := index.Search(key)
			if err != nil {
				t.Fatalf("phase4: failed to search key %s: %v", key, err)
			}
			if val != value() {
				t.Errorf("phase4: expected %d for key %s, got %d", value(), key, val)
			}
			keyCounter++
		}
	})
}
