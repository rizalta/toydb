// Package index
package index

import (
	"errors"
	"fmt"
	"strings"
)

type NodeType uint8

const (
	MaxKeys = 8
	MinKeys = MaxKeys / 2

	NodeTypeInternal = iota
	NodeTypeLeaf
)

var ErrKeyNotFound = errors.New("index: key not found")

type node struct {
	nodeType NodeType
	keys     []uint64
	values   []uint64
	children []*node
	next     *node
}

type BTree struct {
	root *node
}

func newLeafNode() *node {
	return &node{
		nodeType: NodeTypeLeaf,
		keys:     make([]uint64, 0, MaxKeys),
		values:   make([]uint64, 0, MaxKeys),
		children: nil,
		next:     nil,
	}
}

func newInternalNode() *node {
	return &node{
		nodeType: NodeTypeInternal,
		keys:     make([]uint64, 0, MaxKeys),
		values:   nil,
		children: make([]*node, 0, MaxKeys+1),
		next:     nil,
	}
}

func NewBTree() *BTree {
	return &BTree{
		root: newLeafNode(),
	}
}

func (bt *BTree) Search(key uint64) (uint64, error) {
	if bt.root == nil {
		return 0, ErrKeyNotFound
	}

	n := bt.root

	for n.nodeType == NodeTypeInternal {
		i := 0
		for i < len(n.keys) && key >= n.keys[i] {
			i++
		}
		n = n.children[i]
	}

	for i, k := range n.keys {
		if k == key {
			return n.values[i], nil
		}
	}

	return 0, ErrKeyNotFound
}

func (bt *BTree) DebugPrint() {
	if bt.root == nil {
		fmt.Println("<empty tree>")
		return
	}
	printNode(bt.root, 0)
}

func printNode(n *node, level int) {
	indent := strings.Repeat("  ", level)

	if n.nodeType == NodeTypeLeaf {
		fmt.Printf("%sLeaf(keys=%v, values=%v)\n", indent, n.keys, n.values)
	} else {
		fmt.Printf("%sInternal(keys=%v)\n", indent, n.keys)
		for _, child := range n.children {
			printNode(child, level+1)
		}
	}
}
