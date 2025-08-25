package index

import (
	"testing"
)

func TestBPlusTreeSearch(t *testing.T) {
	leaf1 := &node{
		nodeType: NodeTypeLeaf,
		keys:     []uint64{1, 2},
		values:   []uint64{100, 200},
	}
	leaf2 := &node{
		nodeType: NodeTypeLeaf,
		keys:     []uint64{3, 4},
		values:   []uint64{300, 400},
	}
	leaf1.next = leaf2

	root := &node{
		nodeType: NodeTypeInternal,
		keys:     []uint64{3},
		children: []*node{leaf1, leaf2},
	}

	btree := &BTree{root: root}

	tests := map[uint64]uint64{
		1: 100,
		2: 200,
		3: 300,
		4: 400,
	}

	for k, expected := range tests {
		if v, err := btree.Search(k); err != nil || v != expected {
			t.Errorf("Search(%d) = %d, %v; want %d, nil", k, v, err, expected)
		}
	}

	if _, err := btree.Search(99); err == nil {
		t.Errorf("Search(99) should not be found")
	}
}
