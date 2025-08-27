package index

func (bt *BTree) Insert(key uint64, value uint64) {
	if bt.root == nil {
		bt.root = newLeafNode()
	}

	promotedKey, newChild := bt.insert(bt.root, key, value)
	if newChild != nil {
		newRoot := newInternalNode()
		newRoot.keys = append(newRoot.keys, promotedKey)
		newRoot.children = append(newRoot.children, bt.root, newChild)
		bt.root = newRoot
	}
}

func (bt *BTree) insert(n *node, key, value uint64) (uint64, *node) {
	if n.nodeType == NodeTypeLeaf {
		i := 0
		for i < len(n.keys) && key > n.keys[i] {
			i++
		}

		if i < len(n.keys) && n.keys[i] == key {
			n.values[i] = value
			return 0, nil
		}

		n.keys = append(n.keys, 0)
		n.values = append(n.values, 0)
		copy(n.keys[i+1:], n.keys[i:])
		copy(n.values[i+1:], n.values[i:])
		n.keys[i] = key
		n.values[i] = value

		if len(n.keys) > MaxKeys {
			return bt.splitNode(n)
		}

		return 0, nil
	}

	i := 0
	for i < len(n.keys) && key >= n.keys[i] {
		i++
	}

	promotedKey, newChild := bt.insert(n.children[i], key, value)
	if newChild != nil {
		n.keys = append(n.keys, 0)
		n.children = append(n.children, nil)
		copy(n.keys[i+1:], n.keys[i:])
		copy(n.children[i+2:], n.children[i+1:])
		n.keys[i] = promotedKey
		n.children[i+1] = newChild

		if len(n.keys) > MaxKeys {
			return bt.splitNode(n)
		}
	}

	return 0, nil
}

func (bt *BTree) splitNode(n *node) (uint64, *node) {
	mid := len(n.keys) / 2

	switch n.nodeType {
	case NodeTypeLeaf:
		rightNode := newLeafNode()
		rightNode.keys = append(rightNode.keys, n.keys[mid:]...)
		rightNode.values = append(rightNode.values, n.values[mid:]...)
		n.keys = n.keys[:mid]
		n.values = n.values[:mid]
		rightNode.next = n.next
		n.next = rightNode
		promotedKey := rightNode.keys[0]
		return promotedKey, rightNode

	case NodeTypeInternal:
		rightNode := newInternalNode()
		rightNode.keys = append(rightNode.keys, n.keys[mid+1:]...)
		rightNode.children = append(rightNode.children, n.children[mid+1:]...)
		promotedKey := n.keys[mid]
		n.keys = n.keys[:mid]
		n.children = n.children[:mid+1]
		return promotedKey, rightNode
	default:
		return 0, nil
	}
}
