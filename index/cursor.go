package index

import (
	"bytes"
	"sort"

	"github.com/rizalta/toydb/pager"
)

type Cursor struct {
	index  *Index
	pageID pager.PageID
	endKey []byte
	keyNum int
	isEnd  bool
}

func (idx *Index) NewCursor(startKey, endKey []byte) (*Cursor, error) {
	if idx.root == 0 {
		return &Cursor{isEnd: true}, nil
	}

	pageID := idx.root
	n, _, err := idx.readNode(pageID)
	if err != nil {
		return nil, err
	}
	keyNum := 0

	if startKey == nil {
		for n.nodeType == NodeTypeInternal {
			pageID = n.children[0]
			n, _, err = idx.readNode(pageID)
			if err != nil {
				return nil, err
			}
		}
	} else {
		for n.nodeType == NodeTypeInternal {
			i := sort.Search(len(n.keys), func(j int) bool {
				return bytes.Compare(n.keys[j], startKey) > 0
			})
			pageID = n.children[i]
			n, _, err = idx.readNode(pageID)
			if err != nil {
				return nil, err
			}
		}

		keyNum = sort.Search(len(n.keys), func(j int) bool {
			return bytes.Compare(n.keys[j], startKey) >= 0
		})
		if keyNum >= len(n.keys) {
			pageID = n.next
			keyNum = 0
		}
	}

	return &Cursor{
		index:  idx,
		pageID: pageID,
		keyNum: keyNum,
		isEnd:  pageID == 0,
		endKey: endKey,
	}, nil
}

func (c *Cursor) Next() ([]byte, uint64, error) {
	for {
		if c.isEnd {
			return nil, 0, nil
		}

		n, _, err := c.index.readNode(c.pageID)
		if err != nil {
			return nil, 0, err
		}

		if c.keyNum < len(n.keys) {
			key := n.keys[c.keyNum]
			if c.endKey != nil && bytes.Compare(key, c.endKey) >= 0 {
				c.isEnd = true
				return nil, 0, nil
			}
			value := n.values[c.keyNum]
			c.keyNum++
			return key, value, nil
		}

		c.pageID = n.next
		c.keyNum = 0

		if c.pageID == 0 {
			c.isEnd = true
			return nil, 0, nil
		}
	}
}
