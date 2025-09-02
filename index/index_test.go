package index

import (
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/rizalta/toydb/pager"
)

type MockPager struct {
	pages    map[pager.PageID][pager.PageSize]byte
	numPages uint32
	closed   bool
}

func (mp *MockPager) NewPage() (*pager.Page, error) {
	p := &pager.Page{}
	p.ID = pager.PageID(mp.numPages)
	for i := range p.Data {
		p.Data[i] = 0
	}
	if err := mp.WritePage(p); err != nil {
		return nil, err
	}
	mp.numPages++
	return p, nil
}

func (mp *MockPager) ReadPage(pageID pager.PageID) (*pager.Page, error) {
	if p, ok := mp.pages[pageID]; ok {
		return &pager.Page{ID: pageID, Data: p}, nil
	} else {
		return nil, fmt.Errorf("invalid page id")
	}
}

func (mp *MockPager) WritePage(page *pager.Page) error {
	mp.pages[page.ID] = page.Data
	return nil
}

func (mp *MockPager) GetNumPages() uint32 {
	return mp.numPages
}

func (mp *MockPager) Close() error {
	mp.closed = true
	return nil
}

func NewMockPager() *MockPager {
	return &MockPager{
		pages:    make(map[pager.PageID][4096]byte),
		numPages: 0,
	}
}

func TestNewIndex(t *testing.T) {
	p := NewMockPager()
	index, err := NewIndex(p)
	if err != nil {
		t.Fatalf("failed to initialize index: %v", err)
	}
	defer index.Close()

	if index == nil {
		t.Fatalf("expected index got nil")
	}

	if index.root != 1 {
		t.Fatalf("expected rootID 1 but got %d", index.root)
	}

	if p.numPages != 2 {
		t.Fatalf("expected pager numPages to be 2, but got %d", p.numPages)
	}

	meta, err := p.ReadPage(0)
	if err != nil || meta == nil {
		t.Fatalf("page 0 should exist, got %v and err %v", meta, err)
	}

	rootPageID := binary.LittleEndian.Uint32(meta.Data[:])
	if rootPageID != 1 {
		t.Fatalf("root page id from meta should be 1, got %d", rootPageID)
	}

	root, _, err := index.readNode(pager.PageID(rootPageID))
	if err != nil {
		t.Fatalf("failed to read node from root id 1")
	}

	if root.nodeType != NodeTypeLeaf {
		t.Fatalf("root node type should be leaf, got internal")
	}

	if len(root.keys) != 0 {
		t.Fatalf("root node should have 0 keys, got %d", len(root.keys))
	}
}

func TestNewIndexForExistingPager(t *testing.T) {
	p := NewMockPager()
	meta := &pager.Page{ID: 0}
	binary.LittleEndian.PutUint32(meta.Data[:], 42)
	p.pages[meta.ID] = meta.Data
	rootPage := &pager.Page{ID: 42}
	p.pages[rootPage.ID] = rootPage.Data
	p.numPages = 43

	index, err := NewIndex(p)
	if err != nil {
		t.Fatalf("failed to initialize index: %v", err)
	}
	defer index.Close()
	if index == nil {
		t.Fatalf("expected index got nil")
	}
	if index.root != 42 {
		t.Fatalf("expected root page id to be 42, but got %d", index.root)
	}
	if p.GetNumPages() != 43 {
		t.Fatalf("expected 43 pages in pager, got %d", p.GetNumPages())
	}
}

func TestSearchEmptyIndex(t *testing.T) {
	p := NewMockPager()

	index, err := NewIndex(p)
	if err != nil {
		t.Fatalf("failed to initialize index: %v", err)
	}
	defer index.Close()
	if index == nil {
		t.Fatalf("expected index got nil")
	}
	_, err = index.Search(100)
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected %v, got %v", ErrKeyNotFound, err)
	}
}

func TestSearchSingleNode(t *testing.T) {
	p := NewMockPager()

	index, err := NewIndex(p)
	if err != nil {
		t.Fatalf("failed to initialize index: %v", err)
	}
	defer index.Close()
	if index == nil {
		t.Fatalf("expected index got nil")
	}

	root, rootPage, _ := index.readNode(1)
	root.keys = []uint64{10, 20, 30}
	root.values = []uint64{100, 200, 300}

	err = index.writeNode(rootPage, root)
	if err != nil {
		t.Fatalf("failed to write root node")
	}

	tests := []struct {
		name          string
		key           uint64
		expectedValue uint64
		expectedErr   error
	}{
		{
			name:          "Find existing key in middle",
			key:           20,
			expectedValue: 200,
			expectedErr:   nil,
		},
		{
			name:          "Find first existing key",
			key:           10,
			expectedValue: 100,
			expectedErr:   nil,
		},
		{
			name:          "Find last existing key",
			key:           30,
			expectedValue: 300,
			expectedErr:   nil,
		},
		{
			name:          "Find non-existent key (larger)",
			key:           140,
			expectedValue: 0,
			expectedErr:   ErrKeyNotFound,
		},
		{
			name:          "Find non-existent key (smaller)",
			key:           5,
			expectedValue: 0,
			expectedErr:   ErrKeyNotFound,
		},
		{
			name:          "Find non-existent key (between)",
			key:           25,
			expectedValue: 0,
			expectedErr:   ErrKeyNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := index.Search(tt.key)
			if tt.expectedErr != nil {
				if !errors.Is(err, tt.expectedErr) {
					t.Fatalf("expected %v error, got %v", tt.expectedErr, err)
				}
			} else {
				if val != tt.expectedValue {
					t.Fatalf("expected value %d, got %d", tt.expectedValue, val)
				}
			}
		})
	}
}

func TestSearchMultipleNodes(t *testing.T) {
	p := NewMockPager()

	index, err := NewIndex(p)
	if err != nil {
		t.Fatalf("failed to initialize index: %v", err)
	}
	defer index.Close()
	if index == nil {
		t.Fatalf("expected index got nil")
	}

	leaf1 := newLeafNode()
	leaf1.keys = []uint64{10, 20}
	leaf1.values = []uint64{100, 200}
	leaf2 := newLeafNode()
	leaf2.keys = []uint64{30, 40}
	leaf2.values = []uint64{300, 400}
	page1, _ := p.NewPage()
	page2, _ := p.NewPage()
	leaf1.next = page2.ID
	root, rootPage, _ := index.readNode(1)
	root.keys = []uint64{30}
	root.children = []pager.PageID{page1.ID, page2.ID}
	root.nodeType = NodeTypeInternal
	index.writeNode(page1, leaf1)
	index.writeNode(page2, leaf2)
	index.writeNode(rootPage, root)

	tests := []struct {
		name          string
		key           uint64
		expectedValue uint64
		expectedErr   error
	}{
		{
			name:          "Find key in first leaf",
			key:           20,
			expectedValue: 200,
			expectedErr:   nil,
		},
		{
			name:          "Find key in second leaf",
			key:           40,
			expectedValue: 400,
			expectedErr:   nil,
		},
		{
			name:          "Find the separator key",
			key:           30,
			expectedValue: 300,
			expectedErr:   nil,
		},
		{
			name:          "Find non-existent key (would be in first leaf)",
			key:           15,
			expectedValue: 0,
			expectedErr:   ErrKeyNotFound,
		},
		{
			name:          "Find non-existent key (would be in second leaf)",
			key:           35,
			expectedValue: 0,
			expectedErr:   ErrKeyNotFound,
		},
		{
			name:          "Find non-existent key (smaller than all keys)",
			key:           5,
			expectedValue: 0,
			expectedErr:   ErrKeyNotFound,
		},
		{
			name:          "Find non-existent key (larger than all keys)",
			key:           50,
			expectedValue: 0,
			expectedErr:   ErrKeyNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := index.Search(tt.key)
			if tt.expectedErr != nil {
				if !errors.Is(err, tt.expectedErr) {
					t.Fatalf("expected error: %v, got %v", tt.expectedErr, err)
				}
			}
			if val != tt.expectedValue {
				t.Fatalf("expected value %d for key %d, got %d", tt.expectedValue, tt.key, val)
			}
		})
	}
}
