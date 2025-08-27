package index

import (
	"testing"
)

func TestDeleteLeafNoUnderflow(t *testing.T) {
	bt := NewBTree()
	bt.Insert(10, 100)
	bt.Insert(20, 200)
	bt.Insert(5, 50)

	if err := bt.Delete(10); err != nil {
		t.Fatalf("expected to delete key 10, got error: %v", err)
	}

	// Check remaining keys
	tests := []struct {
		key, expected uint64
		found         bool
	}{
		{5, 50, true},
		{20, 200, true},
		{10, 0, false},
	}

	for _, tt := range tests {
		v, err := bt.Search(tt.key)
		if tt.found && err != nil {
			t.Errorf("expected key %d to exist, got error: %v", tt.key, err)
		}
		if !tt.found && err == nil {
			t.Errorf("expected key %d to be deleted", tt.key)
		}
		if tt.found && v != tt.expected {
			t.Errorf("key %d: expected value %d, got %d", tt.key, tt.expected, v)
		}
	}
}

func TestDeleteBorrowLeftRight(t *testing.T) {
	bt := NewBTree()
	// Insert enough keys to force splits
	for i := uint64(1); i <= 20; i++ {
		bt.Insert(i, i*10)
	}

	// Delete keys from leaf to trigger borrow
	if err := bt.Delete(2); err != nil {
		t.Fatalf("delete error: %v", err)
	}
	if err := bt.Delete(3); err != nil {
		t.Fatalf("delete error: %v", err)
	}

	// Check that all remaining keys exist
	for i := uint64(1); i <= 20; i++ {
		if i == 2 || i == 3 {
			if _, err := bt.Search(i); err == nil {
				t.Errorf("key %d should have been deleted", i)
			}
		} else {
			v, err := bt.Search(i)
			if err != nil {
				t.Errorf("key %d should exist, got error: %v", i, err)
			}
			if v != i*10 {
				t.Errorf("key %d: expected %d, got %d", i, i*10, v)
			}
		}
	}
}

func TestDeleteMergeLeaves(t *testing.T) {
	bt := NewBTree()
	for i := uint64(1); i <= 8; i++ {
		bt.Insert(i, i*100)
	}

	// Delete enough to cause merge
	for i := uint64(1); i <= 4; i++ {
		if err := bt.Delete(i); err != nil {
			t.Fatalf("delete error: %v", err)
		}
	}

	// Remaining keys
	for i := uint64(5); i <= 8; i++ {
		v, err := bt.Search(i)
		if err != nil {
			t.Errorf("key %d should exist, got error: %v", i, err)
		}
		if v != i*100 {
			t.Errorf("key %d: expected %d, got %d", i, i*100, v)
		}
	}

	// Deleted keys
	for i := uint64(1); i <= 4; i++ {
		if _, err := bt.Search(i); err == nil {
			t.Errorf("key %d should have been deleted", i)
		}
	}
}

func TestDeleteRootCollapse(t *testing.T) {
	bt := NewBTree()
	bt.Insert(10, 100)
	bt.Insert(20, 200)

	if err := bt.Delete(10); err != nil {
		t.Fatalf("delete error: %v", err)
	}
	if err := bt.Delete(20); err != nil {
		t.Fatalf("delete error: %v", err)
	}

	if bt.root == nil {
		t.Fatalf("root should exist even if empty leaf")
	}
	if len(bt.root.keys) != 0 {
		t.Fatalf("root keys should be empty after deleting all elements")
	}
}
