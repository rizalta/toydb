package storage

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/rizalta/toydb/index"
)

type MockPager struct {
	data   map[uint64][]byte
	closed bool
}

func NewMockPager() *MockPager {
	return &MockPager{
		data:   make(map[uint64][]byte),
		closed: false,
	}
}

func (mp *MockPager) WriteAtOffset(offset uint64, data []byte) error {
	if mp.closed {
		return fmt.Errorf("pager is closed")
	}
	mp.data[offset] = make([]byte, len(data))
	copy(mp.data[offset], data)
	return nil
}

func (mp *MockPager) ReadAtOffset(offset uint64, size int) ([]byte, error) {
	if mp.closed {
		return nil, fmt.Errorf("pager is closed")
	}

	var pageData []byte
	var startOffset uint64
	for off, data := range mp.data {
		if offset >= off && offset < off+uint64(len(data)) {
			pageData = data
			startOffset = off
			break
		}
	}

	if pageData == nil {
		return nil, fmt.Errorf("no data found at offset %d", offset)
	}

	localOffset := offset - startOffset
	endOffset := localOffset + uint64(size)

	if endOffset > uint64(len(pageData)) {
		return nil, fmt.Errorf("read out of bounds")
	}

	return pageData[localOffset:endOffset], nil
}

func (mp *MockPager) Close() error {
	mp.closed = true
	return nil
}

type MockIndex struct {
	index map[uint64]uint64
}

func NewMockIndex() *MockIndex {
	return &MockIndex{
		index: make(map[uint64]uint64),
	}
}

func (mi *MockIndex) Insert(key uint64, value uint64) error {
	mi.index[key] = value
	return nil
}

func (mi *MockIndex) Search(key uint64) (uint64, error) {
	if v, ok := mi.index[key]; ok {
		return v, nil
	}
	return 0, index.ErrKeyNotFound
}

func (mi *MockIndex) Delete(key uint64) error {
	delete(mi.index, key)
	return nil
}

func TestNewStore(t *testing.T) {
	pager := NewMockPager()
	index := NewMockIndex()
	store := NewStore(pager, index)
	defer store.Close()

	if store.offset != 0 {
		t.Fatalf("New store with new pager and index should have offset as 0, but got %d", store.offset)
	}
}

func TestPutGet(t *testing.T) {
	pager := NewMockPager()
	index := NewMockIndex()
	store := NewStore(pager, index)
	defer store.Close()

	key := "test"
	value := []byte("value")

	err := store.Put(key, value)
	if err != nil {
		t.Fatalf("Put for key %s and value %s should not get any error, got %v", key, value, err)
	}

	val, found, err := store.Get("test")
	if err != nil {
		t.Fatalf("Get for key %s should not get any error, got %v", key, err)
	}
	if !found {
		t.Fatalf("Get for key %s should find the value", key)
	}
	if !bytes.Equal(val, value) {
		t.Errorf("expected value %s for key %s, but got %s", value, key, val)
	}
}

func TestMultiplePutGets(t *testing.T) {
	puts := []struct {
		key   string
		value []byte
	}{
		{
			key:   "key1",
			value: []byte("value1"),
		},
		{
			key:   "key2",
			value: []byte("value2"),
		},
		{
			key:   "key3",
			value: []byte("value3"),
		},
		{
			key:   "key4",
			value: []byte("value4"),
		},
		{
			key:   "key5",
			value: []byte("value5"),
		},
		{
			key:   "key2",
			value: []byte("new_value"),
		},
	}

	pager := NewMockPager()
	index := NewMockIndex()
	store := NewStore(pager, index)
	defer store.Close()

	for _, p := range puts {
		t.Run("Put_"+p.key, func(t *testing.T) {
			if err := store.Put(p.key, p.value); err != nil {
				t.Fatalf("failed to put (%s, %s) in store", p.key, p.value)
			}
		})
	}

	expected := []struct {
		key   string
		value []byte
	}{
		{
			key:   "key1",
			value: []byte("value1"),
		},
		{
			key:   "key2",
			value: []byte("new_value"),
		},
		{
			key:   "key3",
			value: []byte("value3"),
		},
		{
			key:   "key4",
			value: []byte("value4"),
		},
		{
			key:   "key5",
			value: []byte("value5"),
		},
	}

	for _, ev := range expected {
		t.Run("Get_"+ev.key, func(t *testing.T) {
			val, found, err := store.Get(ev.key)
			if err != nil {
				t.Fatalf("expected value for key %s but not found", ev.key)
			}
			if !found {
				t.Fatalf("expected value for key %s but not found", ev.key)
			}
			if !bytes.Equal(val, ev.value) {
				t.Errorf("expected value %s for key %s, but got %s", ev.value, ev.key, val)
			}
		})
	}
}

func TestRecovery(t *testing.T) {
	tests := []struct {
		key   string
		value []byte
	}{
		{
			key:   "key1",
			value: []byte("value1"),
		},
		{
			key:   "key2",
			value: []byte("value2"),
		},
		{
			key:   "key3",
			value: []byte("value3"),
		},
		{
			key:   "key4",
			value: []byte("value4"),
		},
		{
			key:   "key5",
			value: []byte("value5"),
		},
	}

	pager := NewMockPager()
	index := NewMockIndex()
	store := NewStore(pager, index)
	defer store.Close()

	for _, tt := range tests {
		if err := store.Put(tt.key, tt.value); err != nil {
			t.Fatalf("failed to put (%s, %s) in store", tt.key, tt.value)
		}
	}

	defer store.Close()

	newIndex := NewMockIndex()
	newStore := NewStore(pager, newIndex)
	defer newStore.Close()

	for _, tt := range tests {
		t.Run("Recovered_Get_"+tt.key, func(t *testing.T) {
			val, found, err := newStore.Get(tt.key)
			if err != nil {
				t.Fatalf("expected value for key %s but not found: %v", tt.key, err)
			}
			if !found {
				t.Fatalf("expected value for key %s but not found", tt.key)
			}
			if !bytes.Equal(val, tt.value) {
				t.Errorf("expected value %s for key %s, but got %s", tt.value, tt.key, val)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	tests := []struct {
		key   string
		value []byte
	}{
		{
			key:   "key1",
			value: []byte("value1"),
		},
		{
			key:   "key2",
			value: []byte("value2"),
		},
		{
			key:   "key3",
			value: []byte("value3"),
		},
		{
			key:   "key4",
			value: []byte("value4"),
		},
		{
			key:   "key5",
			value: []byte("value5"),
		},
	}

	pager := NewMockPager()
	index := NewMockIndex()
	store := NewStore(pager, index)
	defer store.Close()

	for _, tt := range tests {
		if err := store.Put(tt.key, tt.value); err != nil {
			t.Fatalf("failed to put (%s, %s) in store", tt.key, tt.value)
		}
	}

	deleteKey := "key3"
	success, err := store.Delete(deleteKey)
	if err != nil {
		t.Fatalf("failed to delete key %s: %v", deleteKey, err)
	}
	if !success {
		t.Fatalf("should be able to delete the key %s", deleteKey)
	}

	expected := []struct {
		key   string
		value []byte
	}{
		{
			key:   "key1",
			value: []byte("value1"),
		},
		{
			key:   "key2",
			value: []byte("value2"),
		},
		{
			key:   "key4",
			value: []byte("value4"),
		},
		{
			key:   "key5",
			value: []byte("value5"),
		},
	}

	for _, ev := range expected {
		t.Run("Get_after_delete"+ev.key, func(t *testing.T) {
			val, found, err := store.Get(ev.key)
			if err != nil {
				t.Fatalf("expected value for key %s but not found", ev.key)
			}
			if !found {
				t.Fatalf("expected value for key %s but not found", ev.key)
			}
			if !bytes.Equal(val, ev.value) {
				t.Fatalf("expected value %s for key %s, but got %s", ev.value, ev.key, val)
			}
		})
	}

	t.Run("Get_afrer_delete_"+deleteKey, func(t *testing.T) {
		val, found, err := store.Get(deleteKey)
		if err != nil {
			t.Fatalf("expected nil when if key not found")
		}
		if found {
			t.Fatalf("should not be able to find key %s after delete", deleteKey)
		}
		if val != nil {
			t.Fatalf("value for key %s should be nil, but got %s", deleteKey, val)
		}
	})

	t.Run("delete_same_key_again", func(t *testing.T) {
		success, err = store.Delete(deleteKey)
		if err != nil {
			t.Fatalf("error should be nil when deleting again: %v", err)
		}
		if success {
			t.Errorf("should not be able to delete the key again")
		}
	})
}

func TestRecoveryAfterDelete(t *testing.T) {
	puts := []struct {
		key   string
		value []byte
	}{
		{
			key:   "key1",
			value: []byte("value1"),
		},
		{
			key:   "key2",
			value: []byte("value2"),
		},
		{
			key:   "key3",
			value: []byte("value3"),
		},
		{
			key:   "key4",
			value: []byte("value4"),
		},
		{
			key:   "key5",
			value: []byte("value5"),
		},
	}

	pager := NewMockPager()
	index := NewMockIndex()
	store := NewStore(pager, index)
	defer store.Close()

	for _, p := range puts {
		if err := store.Put(p.key, p.value); err != nil {
			t.Fatalf("failed to put (%s, %s) in store", p.key, p.value)
		}
	}

	expected := []struct {
		key   string
		value []byte
	}{
		{
			key:   "key1",
			value: []byte("value1"),
		},
		{
			key:   "key2",
			value: []byte("value2"),
		},
		{
			key:   "key4",
			value: []byte("value4"),
		},
		{
			key:   "key5",
			value: []byte("value5"),
		},
	}

	deleteKey := "key3"
	success, err := store.Delete(deleteKey)
	if err != nil {
		t.Fatalf("failed to delete key %s: %v", deleteKey, err)
	}
	if !success {
		t.Fatalf("should be able to delete the key %s", deleteKey)
	}

	newIndex := NewMockIndex()
	newStore := NewStore(pager, newIndex)
	defer newStore.Close()

	for _, ev := range expected {
		t.Run("Get_after_delete_recovery"+ev.key, func(t *testing.T) {
			val, found, err := newStore.Get(ev.key)
			if err != nil {
				t.Fatalf("expected value for key %s but not found", ev.key)
			}
			if !found {
				t.Fatalf("expected value for key %s but not found", ev.key)
			}
			if !bytes.Equal(val, ev.value) {
				t.Fatalf("expected value %s for key %s, but got %s", ev.value, ev.key, val)
			}
		})
	}

	t.Run("Get_afrer_delete_recovery"+deleteKey, func(t *testing.T) {
		val, found, err := newStore.Get(deleteKey)
		if err != nil {
			t.Fatalf("expected nil when if key not found")
		}
		if found {
			t.Fatalf("should not be able to find key %s after delete", deleteKey)
		}
		if val != nil {
			t.Fatalf("value for key %s should be nil, but got %s", deleteKey, val)
		}
	})
}
