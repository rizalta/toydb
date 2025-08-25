package index

import (
	"testing"
)

// --- invariants ---

func checkNode(t *testing.T, n *node, min, max *uint64, isRoot bool) {
	// keys sorted
	for i := 1; i < len(n.keys); i++ {
		if n.keys[i-1] >= n.keys[i] {
			t.Fatalf("keys not sorted in node: %v", n.keys)
		}
	}

	// no overflow
	if len(n.keys) > MaxKeys {
		t.Fatalf("node has too many keys: %d > %d", len(n.keys), MaxKeys)
	}

	// bounds check
	if min != nil && len(n.keys) > 0 && n.keys[0] < *min {
		t.Fatalf("node key %d < min bound %d", n.keys[0], *min)
	}
	if max != nil && len(n.keys) > 0 && n.keys[len(n.keys)-1] >= *max {
		t.Fatalf("node key %d >= max bound %d", n.keys[len(n.keys)-1], *max)
	}

	switch n.nodeType {
	case NodeTypeLeaf:
		if len(n.keys) != len(n.values) {
			t.Fatalf("leaf keys/values mismatch: %d vs %d", len(n.keys), len(n.values))
		}
		// leaves may have fewer than MinKeys if root
		if !isRoot && len(n.keys) < MinKeys {
			t.Fatalf("leaf underflow: %d < %d", len(n.keys), MinKeys)
		}
	case NodeTypeInternal:
		if len(n.children) != len(n.keys)+1 {
			t.Fatalf("internal children mismatch: %d keys, %d children", len(n.keys), len(n.children))
		}
		// recurse into children
		for i, child := range n.children {
			var childMin, childMax *uint64
			if i > 0 {
				childMin = &n.keys[i-1]
			}
			if i < len(n.keys) {
				childMax = &n.keys[i]
			}
			checkNode(t, child, childMin, childMax, false)
		}
	default:
		t.Fatalf("unknown node type %d", n.nodeType)
	}
}

func checkTree(t *testing.T, bt *BTree) {
	if bt.root == nil {
		return
	}
	checkNode(t, bt.root, nil, nil, true)

	// verify leaf chain ordering
	leaf := bt.root
	for leaf.nodeType == NodeTypeInternal {
		leaf = leaf.children[0]
	}
	for leaf != nil && leaf.next != nil {
		if leaf.keys[len(leaf.keys)-1] >= leaf.next.keys[0] {
			t.Fatalf("leaf chain broken: %v -> %v", leaf.keys, leaf.next.keys)
		}
		leaf = leaf.next
	}
}

// --- tests ---

func TestInsertAndSearch(t *testing.T) {
	bt := NewBTree()
	bt.Insert(10, 100)
	bt.Insert(20, 200)
	bt.Insert(5, 50)

	checkTree(t, bt)
}

func TestSplitAndSearch(t *testing.T) {
	bt := NewBTree()
	for i := range uint64(MaxKeys * 2) {
		bt.Insert(i, i*10)
	}
	checkTree(t, bt)

	// validate searches
	for i := range uint64(MaxKeys * 2) {
		v, err := bt.Search(i)
		if err != nil {
			t.Errorf("missing key %d: %v", i, err)
		}
		if v != i*10 {
			t.Errorf("bad value for %d: expected %d, got %d", i, i*10, v)
		}
	}
}

func TestLeafSplit(t *testing.T) {
	bt := NewBTree() // assume MaxKeys = 3

	// Insert enough keys to force a split in a leaf
	keys := []uint64{10, 20, 30, 40}
	for _, k := range keys {
		bt.Insert(k, k*10)
	}

	// Root should be internal after split
	if bt.root.nodeType != NodeTypeInternal {
		t.Fatalf("expected root to be internal after split, got %v", bt.root.nodeType)
	}

	// Root should have 1 key (the promoted one)
	if len(bt.root.keys) != 1 {
		t.Fatalf("expected root to have 1 key, got %d", len(bt.root.keys))
	}

	// Root should have 2 children
	if len(bt.root.children) != 2 {
		t.Fatalf("expected root to have 2 children, got %d", len(bt.root.children))
	}

	left := bt.root.children[0]
	right := bt.root.children[1]

	if left.nodeType != NodeTypeLeaf || right.nodeType != NodeTypeLeaf {
		t.Fatalf("expected children to be leaf nodes")
	}

	for _, k := range left.keys {
		if k >= bt.root.keys[0] {
			t.Errorf("left child key %d >= root key %d", k, bt.root.keys[0])
		}
	}
	for _, k := range right.keys {
		if k < bt.root.keys[0] {
			t.Errorf("right child key %d < root key %d", k, bt.root.keys[0])
		}
	}
}

func TestInternalSplit(t *testing.T) {
	bt := NewBTree() // assume MaxKeys = 3

	// Insert enough keys to cause a split in root and then in one child
	keys := []uint64{10, 20, 30, 40, 50, 60, 70}
	for _, k := range keys {
		bt.Insert(k, k*10)
	}

	// Root should still be internal
	if bt.root.nodeType != NodeTypeInternal {
		t.Fatalf("expected root to be internal after multiple splits")
	}

	// Root should now have 2 or more keys
	if len(bt.root.keys) < 2 {
		t.Fatalf("expected root to have >=2 keys after internal split, got %d", len(bt.root.keys))
	}

	// Root should have at least 3 children
	if len(bt.root.children) < 3 {
		t.Fatalf("expected root to have >=3 children, got %d", len(bt.root.children))
	}

	// Check ordering property: child key ranges must respect root keys
	for i, child := range bt.root.children {
		for _, k := range child.keys {
			if i > 0 && k < bt.root.keys[i-1] {
				t.Errorf("child %d key %d < root.keys[%d]=%d", i, k, i-1, bt.root.keys[i-1])
			}
			if i < len(bt.root.keys) && k >= bt.root.keys[i] {
				t.Errorf("child %d key %d >= root.keys[%d]=%d", i, k, i, bt.root.keys[i])
			}
		}
	}
}
