package index

import (
	"encoding/binary"

	"github.com/rizalta/toydb/pager"
)

func (bt *BTree) Insert(key uint64, value uint64) error {
	promotedKey, newSiblingID, err := bt.insert(bt.root, key, value)
	if err != nil {
		return err
	}
	if newSiblingID != 0 {
		newRoot := newInternalNode()
		rootPage, err := bt.pager.NewPage()
		if err != nil {
			return err
		}
		newRoot.keys = append(newRoot.keys, promotedKey)
		newRoot.children = append(newRoot.children, bt.root, newSiblingID)
		if err := bt.writeNode(rootPage, newRoot); err != nil {
			return err
		}

		bt.root = rootPage.ID
		meta, err := bt.pager.ReadPage(0)
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint32(meta.Data[:], uint32(rootPage.ID))
		if err := bt.pager.WritePage(meta); err != nil {
			return err
		}
	}
	return nil
}

func (bt *BTree) insert(pageID pager.PageID, key, value uint64) (uint64, pager.PageID, error) {
	n, page, err := bt.readNode(pageID)
	if err != nil {
		return 0, 0, err
	}
	if n.nodeType == NodeTypeLeaf {
		i := 0
		for i < len(n.keys) && key > n.keys[i] {
			i++
		}

		if i < len(n.keys) && n.keys[i] == key {
			n.values[i] = value
			err := bt.writeNode(page, n)
			return 0, 0, err
		}

		n.keys = append(n.keys, 0)
		n.values = append(n.values, 0)
		copy(n.keys[i+1:], n.keys[i:])
		copy(n.values[i+1:], n.values[i:])
		n.keys[i] = key
		n.values[i] = value

		if err := bt.writeNode(page, n); err != nil {
			return 0, 0, err
		}

		if len(n.keys) > MaxKeys {
			return bt.splitNode(pageID)
		}

		return 0, 0, nil
	}

	i := 0
	for i < len(n.keys) && key >= n.keys[i] {
		i++
	}

	promotedKey, newSiblingID, err := bt.insert(n.children[i], key, value)
	if err != nil {
		return 0, 0, err
	}
	if newSiblingID != 0 {
		n.keys = append(n.keys, 0)
		n.children = append(n.children, 0)
		copy(n.keys[i+1:], n.keys[i:])
		copy(n.children[i+2:], n.children[i+1:])
		n.keys[i] = promotedKey
		n.children[i+1] = newSiblingID
		if err := bt.writeNode(page, n); err != nil {
			return 0, 0, err
		}

		if len(n.keys) > MaxKeys {
			return bt.splitNode(pageID)
		}
		return 0, 0, nil
	}

	return 0, 0, nil
}

func (bt *BTree) splitNode(pageID pager.PageID) (uint64, pager.PageID, error) {
	n, page, err := bt.readNode(pageID)
	if err != nil {
		return 0, 0, err
	}
	siblingPage, err := bt.pager.NewPage()
	if err != nil {
		return 0, 0, err
	}
	var siblingNode *node
	mid := len(n.keys) / 2

	var promotedKey uint64
	switch n.nodeType {
	case NodeTypeLeaf:
		siblingNode = newLeafNode()
		siblingNode.keys = append(siblingNode.keys, n.keys[mid:]...)
		siblingNode.values = append(siblingNode.values, n.values[mid:]...)
		n.keys = n.keys[:mid]
		n.values = n.values[:mid]
		siblingNode.next = n.next
		n.next = siblingPage.ID
		promotedKey = siblingNode.keys[0]

	case NodeTypeInternal:
		siblingNode = newInternalNode()
		siblingNode.keys = append(siblingNode.keys, n.keys[mid+1:]...)
		siblingNode.children = append(siblingNode.children, n.children[mid+1:]...)
		promotedKey = n.keys[mid]
		n.keys = n.keys[:mid]
		n.children = n.children[:mid+1]
	}

	if err := bt.writeNode(page, n); err != nil {
		return 0, 0, err
	}

	if err := bt.writeNode(siblingPage, siblingNode); err != nil {
		return 0, 0, err
	}

	return promotedKey, siblingPage.ID, nil
}
