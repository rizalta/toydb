package index

import (
	"fmt"
)

func (bt *BTree) Delete(key uint64) error {
	if bt.root == nil {
		return fmt.Errorf("index: found no root for delete")
	}

	if err := bt.delete(nil, bt.root, key); err != nil {
		return err
	}

	if bt.root.nodeType == NodeTypeInternal && len(bt.root.keys) == 0 {
		if len(bt.root.children) > 0 {
			bt.root = bt.root.children[0]
		} else {
			bt.root = nil
		}
	}

	return nil
}

func (bt *BTree) delete(parent, n *node, key uint64) error {
	i := 0
	for i < len(n.keys) && key > n.keys[i] {
		i++
	}

	if n.nodeType == NodeTypeLeaf {
		if i < len(n.keys) && key == n.keys[i] {
			n.keys = append(n.keys[:i], n.keys[i+1:]...)
			n.values = append(n.values[:i], n.values[i+1:]...)
		} else {
			return fmt.Errorf("index: key not found for delete")
		}

		if parent != nil && len(n.keys) < MinKeys {
			for childIdx, child := range parent.children {
				if child == n {
					bt.fixUnderflow(parent, childIdx)
					break
				}
			}
		}

		return nil
	}

	child := n.children[i]
	err := bt.delete(n, child, key)
	if err != nil {
		return err
	}

	if len(child.keys) < MinKeys {
		bt.fixUnderflow(n, i)
	}

	return nil
}

func (bt *BTree) fixUnderflow(parent *node, childIdx int) {
	child := parent.children[childIdx]
	if childIdx > 0 {
		left := parent.children[childIdx-1]
		if len(left.keys) > MinKeys {
			bt.borrowLeft(left, child)
			parent.keys[childIdx-1] = child.keys[0]
			return
		}
	}

	if childIdx < len(parent.children)-1 {
		right := parent.children[childIdx+1]
		if len(right.keys) > MinKeys {
			bt.borrowRight(right, child)
			parent.keys[childIdx] = right.keys[0]
			return
		}
	}

	if childIdx > 0 {
		left := parent.children[childIdx-1]
		bt.merge(parent, left, child, childIdx-1)
		return
	} else {
		right := parent.children[childIdx+1]
		bt.merge(parent, child, right, childIdx)
	}
}

func (bt *BTree) borrowLeft(left *node, child *node) {
	leftIdx := len(left.keys) - 1
	child.keys = append([]uint64{left.keys[leftIdx]}, child.keys...)
	left.keys = left.keys[:leftIdx]
	if child.nodeType == NodeTypeLeaf {
		child.values = append([]uint64{left.values[leftIdx]}, child.values...)
		left.values = left.values[:leftIdx]
	} else {
		child.children = append([]*node{left.children[leftIdx+1]}, child.children...)
		left.children = left.children[:leftIdx+1]
	}
}

func (bt *BTree) borrowRight(right *node, child *node) {
	child.keys = append(child.keys, right.keys[0])
	right.keys = right.keys[1:]
	if child.nodeType == NodeTypeLeaf {
		child.values = append(child.values, right.values[0])
		right.values = right.values[1:]
	} else {
		child.children = append(child.children, right.children[0])
		right.children = right.children[1:]
	}
}

func (bt *BTree) merge(parent, left, right *node, sepKeyIdx int) {
	if left.nodeType == NodeTypeLeaf {
		left.keys = append(left.keys, right.keys...)
		left.values = append(left.values, right.values...)
		left.next = right.next
	} else {
		left.keys = append(left.keys, parent.keys[sepKeyIdx])
		left.keys = append(left.keys, right.keys...)
		left.children = append(left.children, right.children...)
	}

	parent.keys = append(parent.keys[:sepKeyIdx], parent.keys[sepKeyIdx+1:]...)
	parent.children = append(parent.children[:sepKeyIdx+1], parent.children[sepKeyIdx+2:]...)
}
