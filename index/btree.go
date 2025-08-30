// Package index
package index

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/rizalta/toydb/pager"
)

type NodeType uint8

const (
	MaxKeys = 255
	MinKeys = MaxKeys / 2

	NodeTypeInternal = iota
	NodeTypeLeaf
)

var (
	ErrKeyNotFound      = errors.New("index: key not found")
	ErrChecksumMismatch = errors.New("index: page checksum mismatch")
)

type Pager interface {
	NewPage() (*pager.Page, error)
	ReadPage(pageID pager.PageID) (*pager.Page, error)
	WritePage(page *pager.Page) error
}

type Header struct {
	nodeType NodeType
	keyCount uint16
	next     pager.PageID
	checksum uint32
}

func (h *Header) serialize(data []byte) {
	binary.LittleEndian.PutUint16(data[0:2], uint16(h.nodeType))
	binary.LittleEndian.PutUint16(data[2:4], h.keyCount)
	binary.LittleEndian.PutUint32(data[4:8], uint32(h.next))
	binary.LittleEndian.PutUint32(data[8:12], h.checksum)
}

func (h *Header) deserialize(data []byte) {
	h.nodeType = NodeType(binary.LittleEndian.Uint16(data[0:2]))
	h.keyCount = binary.LittleEndian.Uint16(data[2:4])
	h.next = pager.PageID(binary.LittleEndian.Uint32(data[4:8]))
	h.checksum = binary.LittleEndian.Uint32(data[8:12])
}

type node struct {
	nodeType NodeType
	keys     []uint64
	values   []uint64
	children []pager.PageID
	next     pager.PageID
}

type BTree struct {
	pager Pager
	root  pager.PageID
}

func newLeafNode() *node {
	return &node{
		nodeType: NodeTypeLeaf,
		keys:     make([]uint64, 0, MaxKeys),
		values:   make([]uint64, 0, MaxKeys),
		children: nil,
		next:     0,
	}
}

func newInternalNode() *node {
	return &node{
		nodeType: NodeTypeInternal,
		keys:     make([]uint64, 0, MaxKeys),
		values:   nil,
		children: make([]pager.PageID, 0, MaxKeys+1),
		next:     0,
	}
}

func NewBTree(p Pager) (*BTree, error) {
	meta, err := p.ReadPage(0)
	if err != nil {
		return nil, err
	}

	rootPageID := binary.LittleEndian.Uint32(meta.Data[:])

	return &BTree{
		root:  pager.PageID(rootPageID),
		pager: p,
	}, nil
}

func (bt *BTree) readNode(pageID pager.PageID) (*node, error) {
	page, err := bt.pager.ReadPage(pageID)
	if err != nil {
		return nil, err
	}

	storedChecksum := binary.LittleEndian.Uint32(page.Data[8:12])
	binary.LittleEndian.PutUint32(page.Data[8:12], 0)
	calculatedChecksum := crc32.ChecksumIEEE(page.Data[:])
	if calculatedChecksum != storedChecksum {
		return nil, ErrChecksumMismatch
	}

	binary.LittleEndian.PutUint32(page.Data[8:12], storedChecksum)
	header := &Header{}
	header.deserialize(page.Data[0:16])

	n := &node{
		nodeType: header.nodeType,
		keys:     make([]uint64, header.keyCount),
		next:     header.next,
	}

	keyOffset := 12
	pointersOffset := keyOffset + int(8*header.keyCount)

	for i := range n.keys {
		offset := keyOffset + (i * 8)
		n.keys[i] = binary.LittleEndian.Uint64(page.Data[offset:])
	}

	if n.nodeType == NodeTypeLeaf {
		n.values = make([]uint64, header.keyCount)
		for i := range n.values {
			offset := pointersOffset + (i * 8)
			n.values[i] = binary.LittleEndian.Uint64(page.Data[offset:])
		}
	} else {
		n.children = make([]pager.PageID, header.keyCount+1)
		for i := range n.children {
			offset := pointersOffset + (i * 4)
			n.children[i] = pager.PageID(binary.LittleEndian.Uint64(page.Data[offset:]))
		}
	}

	return n, nil
}

func (bt *BTree) Search(key uint64) (uint64, error) {
	if bt.root == 0 {
		return 0, ErrKeyNotFound
	}

	n, err := bt.readNode(bt.root)
	if err != nil {
		return 0, err
	}

	for n.nodeType == NodeTypeInternal {
		i := 0
		for i < len(n.keys) && key >= n.keys[i] {
			i++
		}
		n, err = bt.readNode(n.children[i])
		if err != nil {
			return 0, err
		}
	}

	for i, k := range n.keys {
		if k == key {
			return n.values[i], nil
		}
	}

	return 0, ErrKeyNotFound
}
