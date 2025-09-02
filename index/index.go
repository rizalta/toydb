// Package index
package index

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sort"

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
	GetNumPages() uint32
	Close() error
}

type Header struct {
	nodeType NodeType
	keyCount uint16
	next     pager.PageID
	checksum uint32
	padding  [4]byte
}

func (h *Header) serialize(data []byte) {
	binary.LittleEndian.PutUint16(data[0:2], uint16(h.nodeType))
	binary.LittleEndian.PutUint16(data[2:4], h.keyCount)
	binary.LittleEndian.PutUint32(data[4:8], uint32(h.next))
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

type Index struct {
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

func NewIndex(p Pager) (*Index, error) {
	if p.GetNumPages() == 0 {
		_, err := p.NewPage()
		if err != nil {
			return nil, err
		}
		rootPage, err := p.NewPage()
		if err != nil {
			return nil, err
		}

		idx := &Index{
			root:  rootPage.ID,
			pager: p,
		}

		if err := idx.updateRootInMeta(); err != nil {
			return nil, err
		}

		rootNode := newLeafNode()
		if err := idx.writeNode(rootPage, rootNode); err != nil {
			return nil, err
		}

		return idx, nil
	}

	meta, err := p.ReadPage(0)
	if err != nil {
		return nil, err
	}

	rootPageID := binary.LittleEndian.Uint32(meta.Data[:])

	return &Index{
		root:  pager.PageID(rootPageID),
		pager: p,
	}, nil
}

func (idx *Index) readNode(pageID pager.PageID) (*node, *pager.Page, error) {
	page, err := idx.pager.ReadPage(pageID)
	if err != nil {
		return nil, nil, err
	}

	storedChecksum := binary.LittleEndian.Uint32(page.Data[8:12])
	binary.LittleEndian.PutUint32(page.Data[8:12], 0)
	calculatedChecksum := crc32.ChecksumIEEE(page.Data[:])
	if calculatedChecksum != storedChecksum {
		return nil, nil, ErrChecksumMismatch
	}

	binary.LittleEndian.PutUint32(page.Data[8:12], storedChecksum)
	header := &Header{}
	header.deserialize(page.Data[0:16])

	n := &node{
		nodeType: header.nodeType,
		keys:     make([]uint64, header.keyCount),
		next:     header.next,
	}

	keyOffset := 16
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
			n.children[i] = pager.PageID(binary.LittleEndian.Uint32(page.Data[offset:]))
		}
	}

	return n, page, nil
}

func (idx *Index) writeNode(page *pager.Page, n *node) error {
	keyCount := len(n.keys)
	header := &Header{
		nodeType: n.nodeType,
		keyCount: uint16(keyCount),
		next:     n.next,
	}

	header.serialize(page.Data[0:16])

	keyOffset := 16
	pointersOffset := keyOffset + (keyCount * 8)

	for i, k := range n.keys {
		offset := keyOffset + (i * 8)
		binary.LittleEndian.PutUint64(page.Data[offset:], k)
	}

	if n.nodeType == NodeTypeLeaf {
		for i, v := range n.values {
			offset := pointersOffset + (i * 8)
			binary.LittleEndian.PutUint64(page.Data[offset:], v)
		}
	} else {
		for i, c := range n.children {
			offset := pointersOffset + (i * 4)
			binary.LittleEndian.PutUint32(page.Data[offset:], uint32(c))
		}
	}

	binary.LittleEndian.PutUint32(page.Data[8:], 0)
	checksum := crc32.ChecksumIEEE(page.Data[:])
	binary.LittleEndian.PutUint32(page.Data[8:], checksum)

	return idx.pager.WritePage(page)
}

func (idx *Index) updateRootInMeta() error {
	meta, err := idx.pager.ReadPage(0)
	if err != nil {
		return err
	}

	binary.LittleEndian.PutUint32(meta.Data[:], uint32(idx.root))

	return idx.pager.WritePage(meta)
}

func (idx *Index) Search(key uint64) (uint64, error) {
	if idx.root == 0 {
		return 0, ErrKeyNotFound
	}

	n, _, err := idx.readNode(idx.root)
	if err != nil {
		return 0, err
	}

	for n.nodeType == NodeTypeInternal {
		i := sort.Search(len(n.keys), func(j int) bool {
			return n.keys[j] > key
		})
		n, _, err = idx.readNode(n.children[i])
		if err != nil {
			return 0, err
		}
	}

	i := sort.Search(len(n.keys), func(j int) bool {
		return n.keys[j] >= key
	})

	if i < len(n.keys) && n.keys[i] == key {
		return n.values[i], nil
	}

	return 0, ErrKeyNotFound
}

func (idx *Index) Close() error {
	return idx.pager.Close()
}
