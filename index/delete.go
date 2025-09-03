package index

import (
	"github.com/rizalta/toydb/pager"
)

func (idx *Index) Delete(key uint64) error {
	if idx.root == 0 {
		return ErrKeyNotFound
	}

	if err := idx.delete(0, idx.root, key); err != nil {
		return err
	}

	root, _, err := idx.readNode(idx.root)
	if err != nil {
		return err
	}
	if root.nodeType == NodeTypeInternal && len(root.keys) == 0 {
		if len(root.children) > 0 {
			idx.root = root.children[0]
		} else {
			idx.root = 0
		}
		if err := idx.updateRootInMeta(); err != nil {
			return err
		}
	}

	return nil
}

func (idx *Index) delete(parentID, pageID pager.PageID, key uint64) error {
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
			if err := idx.writeNode(page, n); err != nil {
				return err
			}
		} else {
			return ErrKeyNotFound
		}
		if parentID != 0 && len(n.keys) < MinKeys {
			return idx.fixUnderflow(parentID, pageID)
		}
	} else {
		childID := n.children[i]
		err = idx.delete(pageID, childID, key)
		if err != nil {
			return err
		}
		child, _, err := idx.readNode(childID)
		if err != nil {
			return err
		}
		if len(child.keys) < MinKeys {
			return idx.fixUnderflow(pageID, childID)
		}
	}

	return nil
}

func (idx *Index) fixUnderflow(parentID, childID pager.PageID) error {
	parentNode, parentPage, err := idx.readNode(parentID)
	if err != nil {
		return err
	}

	childNode, childPage, err := idx.readNode(childID)
	if err != nil {
		return err
	}

	childIdx := -1
	for i, c := range parentNode.children {
		if c == childID {
			childIdx = i
			break
		}
	}

	if childIdx == -1 {
		return ErrKeyNotFound
	}

	if childIdx > 0 {
		leftID := parentNode.children[childIdx-1]
		leftNode, leftPage, err := idx.readNode(leftID)
		if err != nil {
			return err
		}
		if len(leftNode.keys) > MinKeys {
			idx.borrowLeft(parentNode, leftNode, childNode, childIdx-1)
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
			idx.borrowRight(parentNode, rightNode, childNode, childIdx)
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
		return idx.writeNode(parentPage, parentNode)
	} else {
		rightID := parentNode.children[childIdx+1]
		rightNode, _, err := idx.readNode(rightID)
		if err != nil {
			return err
		}
		idx.merge(parentNode, childNode, rightNode, childIdx)
		if err := idx.writeNode(parentPage, parentNode); err != nil {
			return err
		}
		return idx.writeNode(childPage, childNode)
	}
}

func (idx *Index) borrowLeft(parent, left, child *node, sepKeyIdx int) {
	leftIdx := len(left.keys) - 1
	if child.nodeType == NodeTypeLeaf {
		child.keys = append([]uint64{left.keys[leftIdx]}, child.keys...)
		child.values = append([]uint64{left.values[leftIdx]}, child.values...)
		left.keys = left.keys[:leftIdx]
		left.values = left.values[:leftIdx]
		parent.keys[sepKeyIdx] = child.keys[0]
	} else {
		oldSeperator := parent.keys[sepKeyIdx]
		newSeperator := left.keys[leftIdx]
		child.keys = append([]uint64{oldSeperator}, child.keys...)
		child.children = append([]pager.PageID{left.children[leftIdx+1]}, child.children...)
		left.keys = left.keys[:leftIdx]
		left.children = left.children[:leftIdx+1]
		parent.keys[sepKeyIdx] = newSeperator
	}
}

func (idx *Index) borrowRight(parent, right, child *node, sepKeyIdx int) {
	if child.nodeType == NodeTypeLeaf {
		child.keys = append(child.keys, right.keys[0])
		child.values = append(child.values, right.values[0])
		right.keys = right.keys[1:]
		right.values = right.values[1:]
		parent.keys[sepKeyIdx] = right.keys[0]
	} else {
		oldSeperator := parent.keys[sepKeyIdx]
		newSeperator := right.keys[0]
		child.keys = append(child.keys, oldSeperator)
		child.children = append(child.children, right.children[0])
		right.keys = right.keys[1:]
		right.children = right.children[1:]
		parent.keys[sepKeyIdx] = newSeperator
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
