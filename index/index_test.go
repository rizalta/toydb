package index

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"

	"github.com/rizalta/toydb/pager"
)

func newTestIndex(t *testing.T) *Index {
	t.Helper()

	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "index.db")

	p, err := pager.NewPager(indexPath)
	if err != nil {
		t.Fatalf("failed to initialize pager: %v", err)
	}

	idx, err := NewIndex(p)
	if err != nil {
		t.Fatalf("failed to initialize index: %v", err)
	}

	return idx
}

func TestNewIndex(t *testing.T) {
	index := newTestIndex(t)
	defer index.Close()

	if index == nil {
		t.Fatalf("expected index got nil")
	}

	if index.root != 1 {
		t.Fatalf("expected rootID 1 but got %d", index.root)
	}

	if index.pager.GetNumPages() != 2 {
		t.Fatalf("expected pager numPages to be 2, but got %d", index.pager.GetNumPages())
	}

	meta, err := index.pager.ReadPage(0)
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
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "index.db")
	p, err := pager.NewPager(indexPath)
	if err != nil {
		t.Fatalf("failed to initialize pager: %v", err)
	}
	meta := &pager.Page{ID: 0}
	binary.LittleEndian.PutUint32(meta.Data[:], 42)
	err = p.WritePage(meta)
	if err != nil {
		t.Fatalf("failed to write meta page: %v", err)
	}
	rootPage := &pager.Page{ID: 42}
	err = p.WritePage(rootPage)
	if err != nil {
		t.Fatalf("failed to write root page: %v", err)
	}
	p.Close()
	p, _ = pager.NewPager(indexPath)
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
	index := newTestIndex(t)
	defer index.Close()
	if index == nil {
		t.Fatalf("expected index got nil")
	}
	_, err := index.Search(100)
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected %v, got %v", ErrKeyNotFound, err)
	}
}

func TestSearchSingleNode(t *testing.T) {
	index := newTestIndex(t)
	defer index.Close()
	if index == nil {
		t.Fatalf("expected index got nil")
	}

	root, rootPage, _ := index.readNode(1)
	root.keys = []uint64{10, 20, 30}
	root.values = []uint64{100, 200, 300}

	err := index.writeNode(rootPage, root)
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
