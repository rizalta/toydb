package index

import (
	"github.com/rizalta/toydb/pager"
)

type Cursor struct {
	index  *Index
	pageID pager.PageID
	keyNum int
	isEnd  bool
}

func (idx *Index) NewCursor() (*Cursor, error) {
	pageID := idx.root
	n, _, err := idx.readNode(pageID)
	if err != nil {
		return nil, err
	}

	for n.nodeType == NodeTypeInternal {
		pageID = n.children[0]
		n, _, err = idx.readNode(pageID)
		if err != nil {
			return nil, err
		}
	}

	return &Cursor{
		index:  idx,
		pageID: pageID,
		keyNum: 0,
		isEnd:  len(n.keys) == 0,
	}, nil
}

func (c *Cursor) Next() ([]byte, uint64, error) {
	if c.isEnd {
		return nil, 0, nil
	}

	n, _, err := c.index.readNode(c.pageID)
	if err != nil {
		return nil, 0, err
	}

	key := n.keys[c.keyNum]
	value := n.values[c.keyNum]

	c.keyNum++
	if c.keyNum >= len(n.keys) {
		c.pageID = n.next
		c.keyNum = 0
	}
	if c.pageID == 0 {
		c.isEnd = true
	}

	return key, value, nil
}
