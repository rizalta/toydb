package index

import (
	"fmt"

	"github.com/rizalta/toydb/pager"
)

func (idx *Index) Delete(key uint64) error {
	if idx.root == nil {
		return fmt.Errorf("index: found no root for delete")
	}

	if err := idx.delete(nil, idx.root, key); err != nil {
		return err
	}

	if idx.root.nodeType == NodeTypeInternal && len(idx.root.keys) == 0 {
		if len(idx.root.children) > 0 {
			idx.root = idx.root.children[0]
		} else {
			idx.root = nil
		}
	}

	return nil
}

func (idx *Index) delete(parentID, pageID pager.PageID, key uint64) error {
	parent, parentPage, err := idx.readNode(parentID)
	if err != nil {
		return err
	}
	n, page, err := idx.readNode(pageID)
	if err != nil {
		return err
	}

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
				if child == pageID {
					return idx.fixUnderflow(parent, childIdx)
				}
			}
		}

		return nil
	}

	childID := n.children[i]
	child, childPage, err := idx.readNode(childID)
	if err != nil {
		return err
	}
	err = idx.delete(pageID, childID, key)
	if err != nil {
		return err
	}

	if len(child.keys) < MinKeys {
		idx.fixUnderflow(n, i)
	}

	return nil
}

func (idx *Index) fixUnderflow(parentID pager.PageID, childIdx int) error {
	parentNode, parentPage, err := idx.readNode(parentID)
	if err != nil {
		return err
	}
	childID := parentNode.children[childIdx]
	childNode, childPage, err := idx.readNode(childID)
	if err != nil {
		return err
	}
	if childIdx > 0 {
		leftID := parentNode.children[childIdx-1]
		leftNode, leftPage, err := idx.readNode(leftID)
		if err != nil {
			return err
		}
		if len(leftNode.keys) > MinKeys {
			idx.borrowLeft(leftNode, childNode)
			parentNode.keys[childIdx-1] = childNode.keys[0]
			if err := idx.writeNode(leftPage, leftNode); err != nil {
				return err
			}
			if err := idx.writeNode(parentPage, parentNode); err != nil {
				return err
			}
			return idx.writeNode(childPage, childNode)
		}
	}

	if childIdx < len(parentNode.children)-1 {
		rightID := parentNode.children[childIdx+1]
		rightNode, rightPage, err := idx.readNode(rightID)
		if err != nil {
			return err
		}
		if len(rightNode.keys) > MinKeys {
			idx.borrowRight(rightNode, childNode)
			parentNode.keys[childIdx] = rightNode.keys[0]
			if err := idx.writeNode(rightPage, rightNode); err != nil {
				return err
			}
			if err := idx.writeNode(parentPage, parentNode); err != nil {
				return err
			}
			return idx.writeNode(childPage, childNode)
		}
	}

	if childIdx > 0 {
		leftID := parentNode.children[childIdx-1]
		leftNode, leftPage, err := idx.readNode(leftID)
		if err != nil {
			return err
		}
		idx.merge(parentNode, leftNode, childNode, childIdx-1)
		if err := idx.writeNode(leftPage, leftNode); err != nil {
			return err
		}
		if err := idx.writeNode(parentPage, parentNode); err != nil {
			return err
		}
		return idx.writeNode(childPage, childNode)
	} else {
		rightID := parentNode.children[childIdx+1]
		rightNode, rightPage, err := idx.readNode(rightID)
		if err != nil {
			return err
		}
		idx.merge(parentNode, childNode, rightNode, childIdx)
		if err := idx.writeNode(rightPage, rightNode); err != nil {
			return err
		}
		if err := idx.writeNode(parentPage, parentNode); err != nil {
			return err
		}
		return idx.writeNode(childPage, childNode)
	}
}

func (idx *Index) borrowLeft(left *node, child *node) {
	leftIdx := len(left.keys) - 1
	child.keys = append([]uint64{left.keys[leftIdx]}, child.keys...)
	left.keys = left.keys[:leftIdx]
	if child.nodeType == NodeTypeLeaf {
		child.values = append([]uint64{left.values[leftIdx]}, child.values...)
		left.values = left.values[:leftIdx]
	} else {
		child.children = append([]pager.PageID{left.children[leftIdx+1]}, child.children...)
		left.children = left.children[:leftIdx+1]
	}
}

func (idx *Index) borrowRight(right *node, child *node) {
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

func (idx *Index) merge(parent, left, right *node, sepKeyIdx int) {
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
