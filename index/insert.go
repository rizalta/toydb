package index

import (
	"bytes"
	"sort"

	"github.com/rizalta/toydb/pager"
)

type InsertMode uint8

const (
	Upsert InsertMode = iota
	InsertOnly
	UpdateOnly
)

func (idx *Index) Insert(key []byte, value uint64, inserMode InsertMode) error {
	promotedKey, newSiblingID, err := idx.insert(idx.root, key, value, inserMode)
	if err != nil {
		return err
	}

	if newSiblingID != 0 {
		newRoot := newInternalNode()
		rootPage, err := idx.pager.NewPage()
		if err != nil {
			return err
		}
		newRoot.keys = append(newRoot.keys, promotedKey)
		newRoot.children = append(newRoot.children, idx.root, newSiblingID)
		if err := idx.writeNode(rootPage, newRoot); err != nil {
			return err
		}

		idx.root = rootPage.ID
		if err := idx.syncMetaPage(); err != nil {
			return err
		}
	}

	return nil
}

func (idx *Index) insert(pageID pager.PageID, key []byte, value uint64, inserMode InsertMode) ([]byte, pager.PageID, error) {
	n, page, err := idx.readNode(pageID)
	if err != nil {
		return nil, 0, err
	}

	if n.nodeType == NodeTypeLeaf {
		i := sort.Search(len(n.keys), func(j int) bool {
			return bytes.Compare(n.keys[j], key) >= 0
		})
		if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
			if inserMode == InsertOnly {
				return nil, 0, ErrKeyAlreadyExists
			}

			n.values[i] = value
			err := idx.writeNode(page, n)
			return nil, 0, err
		}

		if inserMode == UpdateOnly {
			return nil, 0, ErrKeyNotFound
		}

		n.keys = append(n.keys, []byte{})
		n.values = append(n.values, 0)
		copy(n.keys[i+1:], n.keys[i:])
		copy(n.values[i+1:], n.values[i:])
		n.keys[i] = key
		n.values[i] = value
		if n.calculateSize() > splitThreshold {
			return idx.splitNode(page, n)
		}

		if err := idx.writeNode(page, n); err != nil {
			return nil, 0, err
		}

		return nil, 0, nil
	}

	i := sort.Search(len(n.keys), func(j int) bool {
		return bytes.Compare(n.keys[j], key) > 0
	})

	promotedKey, newSiblingID, err := idx.insert(n.children[i], key, value, inserMode)
	if err != nil {
		return nil, 0, err
	}

	if newSiblingID != 0 {
		n.keys = append(n.keys, []byte{})
		n.children = append(n.children, 0)
		copy(n.keys[i+1:], n.keys[i:])
		copy(n.children[i+2:], n.children[i+1:])
		n.keys[i] = promotedKey
		n.children[i+1] = newSiblingID

		if n.calculateSize() > splitThreshold {
			return idx.splitNode(page, n)
		}

		if err := idx.writeNode(page, n); err != nil {
			return nil, 0, err
		}

		return nil, 0, nil
	}

	return nil, 0, nil
}

func (idx *Index) splitNode(page *pager.Page, n *node) ([]byte, pager.PageID, error) {
	siblingPage, err := idx.pager.NewPage()
	if err != nil {
		return nil, 0, err
	}

	var siblingNode *node
	mid := len(n.keys) / 2

	var promotedKey []byte
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

	if err := idx.writeNode(page, n); err != nil {
		return nil, 0, err
	}

	if err := idx.writeNode(siblingPage, siblingNode); err != nil {
		return nil, 0, err
	}

	return promotedKey, siblingPage.ID, nil
}
