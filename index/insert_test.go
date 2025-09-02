package index

import (
	"slices"
	"testing"
)

func TestInsertSimple(t *testing.T) {
	p := NewMockPager()
	index, err := NewIndex(p)
	if err != nil {
		t.Fatalf("failed to initialize index: %v", err)
	}
	defer index.Close()
	if index == nil {
		t.Fatalf("expected index, got nil")
	}

	t.Run("Insert_one_key_value", func(t *testing.T) {
		err := index.Insert(10, 100)
		if err != nil {
			t.Fatalf("failed to insert (10, 100) into index")
		}
		val, err := index.Search(10)
		if err != nil {
			t.Fatalf("search for key 10 should not give err: %v", err)
		}
		if val != 100 {
			t.Errorf("expected value 100 for key 10, got %d", val)
		}
	})

	t.Run("Insert_second_key_value", func(t *testing.T) {
		err = index.Insert(5, 50)
		if err != nil {
			t.Fatalf("failed to insert (5, 50) into index")
		}
		val, err := index.Search(5)
		if err != nil {
			t.Fatalf("search for key 5 should not give err: %v", err)
		}
		if val != 50 {
			t.Errorf("expected value 50 for key 5, got %d", val)
		}
		val, err = index.Search(10)
		if err != nil {
			t.Fatalf("search for key 10 should not give err: %v", err)
		}
		if val != 100 {
			t.Errorf("expected value 100 for key 10, got %d", val)
		}
	})

	t.Run("Validate_keys_are_sorted_in_page", func(t *testing.T) {
		root, _, _ := index.readNode(1)
		expectedKeys := []uint64{5, 10}
		if !slices.Equal(root.keys, expectedKeys) {
			t.Errorf("expected keys in root node as %v, got %v", expectedKeys, root.keys)
		}
	})
}

func TestInsertAndUpdate(t *testing.T) {
	p := NewMockPager()
	index, err := NewIndex(p)
	if err != nil {
		t.Fatalf("failed to initialize index: %v", err)
	}
	defer index.Close()
	if index == nil {
		t.Fatalf("expected index, got nil")
	}

	err = index.Insert(10, 100)
	if err != nil {
		t.Fatalf("failed to insert (10, 100) into index")
	}
	err = index.Insert(10, 999)
	if err != nil {
		t.Fatalf("failed to insert second value 999 for key 10")
	}

	val, err := index.Search(10)
	if err != nil {
		t.Fatalf("failed to search for key 10: %v", err)
	}
	if val != 999 {
		t.Errorf("expected updated value 999 for key 10, got %d", val)
	}
}

func TestInsertAndSplit(t *testing.T) {
	p := NewMockPager()
	index, err := NewIndex(p)
	if err != nil {
		t.Fatalf("failed to initialize index: %v", err)
	}
	defer index.Close()
	if index == nil {
		t.Fatalf("expected index, got nil")
	}

	for i := range MaxKeys {
		key := uint64(i)
		value := uint64(1000 + i)
		err := index.Insert(key, value)
		if err != nil {
			t.Fatalf("failed to insert (%d, %d) into index: %v", key, value, err)
		}
	}
	root, _, err := index.readNode(index.root)
	if err != nil {
		t.Fatalf("failed to read root node from page id %d: %v", index.root, err)
	}
	if len(root.keys) != MaxKeys {
		t.Fatalf("expected root node be full with %d keys, but got %d", MaxKeys, len(root.keys))
	}
	splitKey := uint64(MaxKeys)
	splitValue := uint64(MaxKeys + 1000)

	err = index.Insert(splitKey, splitValue)
	if err != nil {
		t.Fatalf("failed to insert (%d, %d) into index which should trigger split: %v", splitKey, splitValue, err)
	}

	t.Run("Verify_root_node", func(t *testing.T) {
		root, _, err = index.readNode(index.root)
		if err != nil {
			t.Fatalf("failed to read root node from page id %d: %v", index.root, err)
		}
		if root.nodeType != NodeTypeInternal {
			t.Errorf("expected root nodeType to be NodeTypeInternal, got %v", root.nodeType)
		}
		if len(root.keys) != 1 {
			t.Errorf("expected num of keys for root node be 1, got %d", len(root.keys))
		}
		if len(root.children) != 2 {
			t.Errorf("expected num of children for root node be 2, got %d", len(root.children))
		}
		expectedKey := uint64((MaxKeys + 1) / 2)
		if root.keys[0] != expectedKey {
			t.Errorf("expected promoted key to be median key %d, got %d", expectedKey, root.keys[0])
		}
	})

	root, _, err = index.readNode(index.root)
	leftChildID := root.children[0]
	rightChildID := root.children[1]

	t.Run("Verify_left_node", func(t *testing.T) {
		leftChild, _, err := index.readNode(leftChildID)
		if err != nil {
			t.Fatalf("failed to read left child node from page id %d: %v", leftChildID, err)
		}
		if leftChild.nodeType != NodeTypeLeaf {
			t.Errorf("expected child nodeType to be NodeTypeLeaf, got %v", leftChild.nodeType)
		}
		expectedKeyCount := (MaxKeys + 1) / 2
		if len(leftChild.keys) != expectedKeyCount {
			t.Errorf("expected left node to have %d keys, got %d", expectedKeyCount, len(leftChild.keys))
		}
		for i := range expectedKeyCount {
			key := uint64(i)
			value := uint64(1000 + i)
			if leftChild.keys[i] != key {
				t.Errorf("expected key %d at index %d, got %d", key, i, leftChild.keys[i])
			}
			if leftChild.values[i] != value {
				t.Errorf("expected value %d at index %d, got %d", value, i, leftChild.values[i])
			}
		}
		if leftChild.next != rightChildID {
			t.Errorf("expected right child id %d to be next of left child, got %d", rightChildID, leftChild.next)
		}
	})

	t.Run("Verify_right_node", func(t *testing.T) {
		rightChild, _, err := index.readNode(rightChildID)
		if err != nil {
			t.Fatalf("failed to read right child node from page id %d: %v", rightChildID, err)
		}
		if rightChild.nodeType != NodeTypeLeaf {
			t.Errorf("expected child nodeType to be NodeTypeLeaf, got %v", rightChild.nodeType)
		}
		expectedKeyCount := (MaxKeys + 1) - ((MaxKeys + 1) / 2)
		if len(rightChild.keys) != expectedKeyCount {
			t.Errorf("expected right node to have %d keys, got %d", expectedKeyCount, len(rightChild.keys))
		}

		for i := range expectedKeyCount {
			startKey := uint64((MaxKeys + 1) / 2)
			startValue := startKey + 1000
			key := startKey + uint64(i)
			value := startValue + uint64(i)
			if rightChild.keys[i] != key {
				t.Errorf("expected key %d at index %d, got %d", key, i, rightChild.keys[i])
			}
			if rightChild.values[i] != value {
				t.Errorf("expected value %d at index %d, got %d", value, i, rightChild.values[i])
			}
		}
		if rightChild.next != 0 {
			t.Errorf("expected 0 to be next of right child, got %d", rightChild.next)
		}
	})

	t.Run("Verify_all_keys_are_searchable", func(t *testing.T) {
		for i := range MaxKeys + 1 {
			key := uint64(i)
			expectedValue := uint64(1000 + i)

			value, err := index.Search(key)
			if err != nil {
				t.Errorf("failed to search for key %d: %v", key, err)
			}

			if value != expectedValue {
				t.Errorf("expected value %d for key %d, got %d", expectedValue, key, value)
			}
		}
	})
}

func TestInternalNodeSplit(t *testing.T) {
	p := NewMockPager()
	index, err := NewIndex(p)
	if err != nil {
		t.Fatalf("failed to initialize index: %v", err)
	}
	defer index.Close()
	if index == nil {
		t.Fatalf("expected index, got nil")
	}

	var keyCounter uint64 = 0
	value := func() uint64 { return keyCounter + 1000 }

	t.Log("Phase1: Creating initial internal root")
	for range MaxKeys + 1 {
		err := index.Insert(keyCounter, value())
		if err != nil {
			t.Fatalf("phase1: failed to insert key %d, : %v", keyCounter, err)
		}
		keyCounter++
	}

	t.Logf("Phase2: Filling internal root node with %d keys", MaxKeys)
	for range MaxKeys - 1 {
		numToFillAndSplit := (MaxKeys + 1) / 2
		for range numToFillAndSplit {
			err = index.Insert(keyCounter, value())
			if err != nil {
				t.Fatalf("phase2: failed to insert key %d, : %v", keyCounter, err)
			}
			keyCounter++
		}
	}
	root, _, _ := index.readNode(index.root)
	if len(root.keys) != MaxKeys {
		t.Fatalf("phase2: expected root node to be filled with %d keys, got %d", MaxKeys, len(root.keys))
	}

	t.Log("Phase3: fill the target leaf node")
	numToFill := MaxKeys / 2
	for range numToFill {
		err := index.Insert(keyCounter, value())
		if err != nil {
			t.Fatalf("phase3: failed to insert key %d, : %v", keyCounter, err)
		}
		keyCounter++
	}

	t.Log("Phase4: trigger the internal node split")
	triggerKey := keyCounter
	err = index.Insert(triggerKey, value())
	if err != nil {
		t.Fatalf("phase4: failed to insert trigger key %d: %v", triggerKey, err)
	}

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
}
