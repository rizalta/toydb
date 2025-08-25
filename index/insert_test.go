package index

import (
	"testing"
)

func checkNode(t *testing.T, n *node, min, max *uint64, isRoot bool) {
	for i := 1; i < len(n.keys); i++ {
		if n.keys[i-1] >= n.keys[i] {
			t.Fatalf("keys not sorted in node: %v", n.keys)
		}
	}

	if len(n.keys) > MaxKeys {
		t.Fatalf("node has too many keys: %d > %d", len(n.keys), MaxKeys)
	}

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

		if !isRoot && len(n.keys) < MinKeys {
			t.Fatalf("leaf underflow: %d < %d", len(n.keys), MinKeys)
		}

	case NodeTypeInternal:
		if len(n.children) != len(n.keys)+1 {
			t.Fatalf("internal children mismatch: %d keys, %d children", len(n.keys), len(n.children))
		}

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

func TestInsertAndSearch(t *testing.T) {
	tests := map[uint64]uint64{
		10: 100,
		20: 200,
		5:  50,
	}
	bt := NewBTree()
	for k, v := range tests {
		bt.Insert(k, v)
	}

	checkTree(t, bt)

	for k, v := range tests {
		val, err := bt.Search(k)
		if err != nil {
			t.Errorf("missing key %d: %v", k, err)
		}
		if v != val {
			t.Errorf("expected %d for key %d, got %d", v, k, val)
		}
	}
}

func TestDuplicateInsert(t *testing.T) {
	tests := map[uint64]uint64{
		10: 100,
		20: 200,
		5:  50,
		30: 200,
	}
	bt := NewBTree()
	for k, v := range tests {
		bt.Insert(k, v)
	}

	bt.Insert(5, 300)
	tests[5] = 300

	checkTree(t, bt)

	for k, v := range tests {
		val, err := bt.Search(k)
		if err != nil {
			t.Errorf("missing key %d: %v", k, err)
		}
		if v != val {
			t.Errorf("expected %d for key %d, got %d", v, k, val)
		}
	}
}

func TestSplitAndSearch(t *testing.T) {
	bt := NewBTree()
	for i := range uint64(MaxKeys * 2) {
		bt.Insert(i, i*10)
	}
	checkTree(t, bt)

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
	bt := NewBTree()

	keys := []uint64{}
	for i := range MaxKeys * 2 {
		keys = append(keys, uint64(i*10))
	}
	for _, k := range keys {
		bt.Insert(k, k*10)
	}

	checkTree(t, bt)

	for i := range MaxKeys * 2 {
		key := uint64(i * 10)
		value := uint64(key * 10)

		val, err := bt.Search(key)
		if err != nil {
			t.Errorf("missing key %d: %v", i, err)
		}
		if val != value {
			t.Errorf("expected value %d for key %d, got %d", value, key, val)
		}
	}
}
