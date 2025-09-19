// Package index
package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sort"

	"github.com/rizalta/toydb/pager"
)

type NodeType uint16

const (
	headerSize = 16
	slotSize   = 2
	valueSize  = 8
	childSize  = 4

	NodeTypeInternal = iota
	NodeTypeLeaf
)

var (
	ErrKeyNotFound      = errors.New("index: key not found")
	ErrChecksumMismatch = errors.New("index: page checksum mismatch")
	ErrKeyAlreadyExists = errors.New("index: key already exists")
)

type Pager interface {
	NewPage() (*pager.Page, error)
	ReadPage(pageID pager.PageID) (*pager.Page, error)
	WritePage(page *pager.Page) error
	GetNumPages() uint32
	FreePage(pageID pager.PageID) error
	GetFreeListID() pager.PageID
	SetFreeListID(pageID pager.PageID)
	Close() error
}

type Header struct {
	nodeType     NodeType
	numSlots     uint16
	freeSpacePtr uint16
	next         pager.PageID
	checksum     uint32
	_            [2]byte
}

func (h *Header) serialize(data []byte) {
	binary.LittleEndian.PutUint16(data[0:2], uint16(h.nodeType))
	binary.LittleEndian.PutUint16(data[2:4], h.numSlots)
	binary.LittleEndian.PutUint16(data[4:6], h.freeSpacePtr)
	binary.LittleEndian.PutUint32(data[6:10], uint32(h.next))
	binary.LittleEndian.PutUint32(data[10:14], h.checksum)
}

func (h *Header) deserialize(data []byte) {
	h.nodeType = NodeType(binary.LittleEndian.Uint16(data[0:2]))
	h.numSlots = binary.LittleEndian.Uint16(data[2:4])
	h.freeSpacePtr = binary.LittleEndian.Uint16(data[4:6])
	h.next = pager.PageID(binary.LittleEndian.Uint32(data[6:10]))
	h.checksum = binary.LittleEndian.Uint32(data[10:14])
}

type node struct {
	nodeType NodeType
	keys     [][]byte
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
		keys:     make([][]byte, 0),
		values:   make([]uint64, 0),
		children: nil,
		next:     0,
	}
}

func newInternalNode() *node {
	return &node{
		nodeType: NodeTypeInternal,
		keys:     make([][]byte, 0),
		values:   nil,
		children: make([]pager.PageID, 0),
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

		if err := idx.syncMetaPage(); err != nil {
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

	rootPageID := pager.PageID(binary.LittleEndian.Uint32(meta.Data[:]))
	freeListID := pager.PageID(binary.LittleEndian.Uint32(meta.Data[4:]))
	p.SetFreeListID(freeListID)

	return &Index{
		root:  rootPageID,
		pager: p,
	}, nil
}

func (idx *Index) readNode(pageID pager.PageID) (*node, *pager.Page, error) {
	page, err := idx.pager.ReadPage(pageID)
	if err != nil {
		return nil, nil, err
	}

	storedChecksum := binary.LittleEndian.Uint32(page.Data[10:14])
	binary.LittleEndian.PutUint32(page.Data[10:14], 0)
	calculatedChecksum := crc32.ChecksumIEEE(page.Data[:])
	if calculatedChecksum != storedChecksum {
		return nil, nil, ErrChecksumMismatch
	}

	binary.LittleEndian.PutUint32(page.Data[10:14], storedChecksum)
	header := &Header{}
	header.deserialize(page.Data[0:headerSize])

	n := &node{
		nodeType: header.nodeType,
		keys:     make([][]byte, header.numSlots),
		next:     header.next,
	}

	slotOffset := headerSize
	endOffset := uint16(pager.PageSize)
	for i := range header.numSlots {
		startOffset := binary.LittleEndian.Uint16(page.Data[slotOffset:])
		key := page.Data[startOffset:endOffset]
		n.keys[i] = make([]byte, len(key))
		copy(n.keys[i], key)
		slotOffset += slotSize
		endOffset = startOffset
	}

	pointersOffset := slotOffset
	if n.nodeType == NodeTypeLeaf {
		n.values = make([]uint64, header.numSlots)
		for i := range n.values {
			n.values[i] = binary.LittleEndian.Uint64(page.Data[pointersOffset:])
			pointersOffset += valueSize
		}
	} else {
		n.children = make([]pager.PageID, header.numSlots+1)
		for i := range n.children {
			n.children[i] = pager.PageID(binary.LittleEndian.Uint32(page.Data[pointersOffset:]))
			pointersOffset += childSize
		}
	}

	return n, page, nil
}

func (idx *Index) writeNode(page *pager.Page, n *node) error {
	for i := range page.Data {
		page.Data[i] = 0
	}

	numSlots := len(n.keys)
	header := &Header{
		nodeType:     n.nodeType,
		numSlots:     uint16(numSlots),
		freeSpacePtr: pager.PageSize,
		next:         n.next,
	}

	slotOffset := headerSize
	for _, key := range n.keys {
		header.freeSpacePtr -= uint16(len(key))
		copy(page.Data[header.freeSpacePtr:], key)
		binary.LittleEndian.PutUint16(page.Data[slotOffset:], header.freeSpacePtr)
		slotOffset += slotSize
	}

	pointersOffset := slotOffset

	if n.nodeType == NodeTypeLeaf {
		for _, v := range n.values {
			binary.LittleEndian.PutUint64(page.Data[pointersOffset:], v)
			pointersOffset += valueSize
		}
	} else {
		for _, c := range n.children {
			binary.LittleEndian.PutUint32(page.Data[pointersOffset:], uint32(c))
			pointersOffset += childSize
		}
	}

	header.serialize(page.Data[:headerSize])

	binary.LittleEndian.PutUint32(page.Data[10:], 0)
	checksum := crc32.ChecksumIEEE(page.Data[:])
	header.checksum = checksum

	header.serialize(page.Data[:headerSize])

	return idx.pager.WritePage(page)
}

func (idx *Index) syncMetaPage() error {
	meta, err := idx.pager.ReadPage(0)
	if err != nil {
		return err
	}

	binary.LittleEndian.PutUint32(meta.Data[:], uint32(idx.root))
	binary.LittleEndian.PutUint32(meta.Data[4:], uint32(idx.pager.GetFreeListID()))

	return idx.pager.WritePage(meta)
}

func (idx *Index) Search(key []byte) (uint64, error) {
	if idx.root == 0 {
		return 0, ErrKeyNotFound
	}

	n, _, err := idx.readNode(idx.root)
	if err != nil {
		return 0, err
	}

	for n.nodeType == NodeTypeInternal {
		i := sort.Search(len(n.keys), func(j int) bool {
			return bytes.Compare(n.keys[j], key) > 0
		})
		n, _, err = idx.readNode(n.children[i])
		if err != nil {
			return 0, err
		}
	}

	i := sort.Search(len(n.keys), func(j int) bool {
		return bytes.Compare(n.keys[j], key) >= 0
	})

	if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
		return n.values[i], nil
	}

	return 0, ErrKeyNotFound
}

func (idx *Index) Close() error {
	if err := idx.syncMetaPage(); err != nil {
		return err
	}
	return idx.pager.Close()
}
