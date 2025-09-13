package storage

import (
	"bytes"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()

	tempDir := t.TempDir()
	store, err := NewStore(tempDir)
	if err != nil {
		t.Fatalf("failed to create temp data dir: %v", err)
	}

	return store
}

func TestNewStore(t *testing.T) {
	store := newTestStore(t)
	defer store.Close()

	if store.offset != 0 {
		t.Fatalf("New store with new pager and index should have offset as 0, but got %d", store.offset)
	}
}

func TestPutGet(t *testing.T) {
	store := newTestStore(t)
	defer store.Close()

	key := []byte("test")
	value := []byte("value")

	err := store.Put(key, value)
	if err != nil {
		t.Fatalf("Put for key %s and value %s should not get any error, got %v", key, value, err)
	}

	val, found, err := store.Get(key)
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
		key   []byte
		value []byte
	}{
		{
			key:   []byte("key1"),
			value: []byte("value1"),
		},
		{
			key:   []byte("key2"),
			value: []byte("value2"),
		},
		{
			key:   []byte("key3"),
			value: []byte("value3"),
		},
		{
			key:   []byte("key4"),
			value: []byte("value4"),
		},
		{
			key:   []byte("key5"),
			value: []byte("value5"),
		},
		{
			key:   []byte("key2"),
			value: []byte("new_value"),
		},
	}

	store := newTestStore(t)
	defer store.Close()

	for _, p := range puts {
		t.Run("Put_"+string(p.key), func(t *testing.T) {
			if err := store.Put(p.key, p.value); err != nil {
				t.Fatalf("failed to put (%s, %s) in store", p.key, p.value)
			}
		})
	}

	expected := []struct {
		key   []byte
		value []byte
	}{
		{
			key:   []byte("key1"),
			value: []byte("value1"),
		},
		{
			key:   []byte("key2"),
			value: []byte("new_value"),
		},
		{
			key:   []byte("key3"),
			value: []byte("value3"),
		},
		{
			key:   []byte("key4"),
			value: []byte("value4"),
		},
		{
			key:   []byte("key5"),
			value: []byte("value5"),
		},
	}

	for _, ev := range expected {
		t.Run("Get_"+string(ev.key), func(t *testing.T) {
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
		key   []byte
		value []byte
	}{
		{
			key:   []byte("key1"),
			value: []byte("value1"),
		},
		{
			key:   []byte("key2"),
			value: []byte("value2"),
		},
		{
			key:   []byte("key3"),
			value: []byte("value3"),
		},
		{
			key:   []byte("key4"),
			value: []byte("value4"),
		},
		{
			key:   []byte("key5"),
			value: []byte("value5"),
		},
	}

	tempDir := t.TempDir()

	store, err := NewStore(tempDir)
	if err != nil {
		t.Fatalf("failed to create a store from temp dir: %v", err)
	}

	for _, tt := range tests {
		if err := store.Put(tt.key, tt.value); err != nil {
			t.Fatalf("failed to put (%s, %s) in store", tt.key, tt.value)
		}
	}

	store.Close()

	newStore, err := NewStore(tempDir)
	if err != nil {
		t.Fatalf("failed to create a new store from temp dir: %v", err)
	}
	defer newStore.Close()

	for _, tt := range tests {
		t.Run("Recovered_Get_"+string(tt.key), func(t *testing.T) {
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
		key   []byte
		value []byte
	}{
		{
			key:   []byte("key1"),
			value: []byte("value1"),
		},
		{
			key:   []byte("key2"),
			value: []byte("value2"),
		},
		{
			key:   []byte("key3"),
			value: []byte("value3"),
		},
		{
			key:   []byte("key4"),
			value: []byte("value4"),
		},
		{
			key:   []byte("key5"),
			value: []byte("value5"),
		},
	}

	store := newTestStore(t)
	defer store.Close()

	for _, tt := range tests {
		if err := store.Put(tt.key, tt.value); err != nil {
			t.Fatalf("failed to put (%s, %s) in store", tt.key, tt.value)
		}
	}

	deleteKey := []byte("key3")
	success, err := store.Delete(deleteKey)
	if err != nil {
		t.Fatalf("failed to delete key %s: %v", deleteKey, err)
	}
	if !success {
		t.Fatalf("should be able to delete the key %s", deleteKey)
	}

	expected := []struct {
		key   []byte
		value []byte
	}{
		{
			key:   []byte("key1"),
			value: []byte("value1"),
		},
		{
			key:   []byte("key2"),
			value: []byte("value2"),
		},
		{
			key:   []byte("key4"),
			value: []byte("value4"),
		},
		{
			key:   []byte("key5"),
			value: []byte("value5"),
		},
	}

	for _, ev := range expected {
		t.Run("Get_after_delete"+string(ev.key), func(t *testing.T) {
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

	t.Run("Get_afrer_delete_"+string(deleteKey), func(t *testing.T) {
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
		key   []byte
		value []byte
	}{
		{
			key:   []byte("key1"),
			value: []byte("value1"),
		},
		{
			key:   []byte("key2"),
			value: []byte("value2"),
		},
		{
			key:   []byte("key3"),
			value: []byte("value3"),
		},
		{
			key:   []byte("key4"),
			value: []byte("value4"),
		},
		{
			key:   []byte("key5"),
			value: []byte("value5"),
		},
	}

	tempDir := t.TempDir()
	store, err := NewStore(tempDir)
	if err != nil {
		t.Fatalf("failed to create a store from temp dir: %v", err)
	}

	for _, p := range puts {
		if err := store.Put(p.key, p.value); err != nil {
			t.Fatalf("failed to put (%s, %s) in store", p.key, p.value)
		}
	}

	expected := []struct {
		key   []byte
		value []byte
	}{
		{
			key:   []byte("key1"),
			value: []byte("value1"),
		},
		{
			key:   []byte("key2"),
			value: []byte("value2"),
		},
		{
			key:   []byte("key4"),
			value: []byte("value4"),
		},
		{
			key:   []byte("key5"),
			value: []byte("value5"),
		},
	}

	deleteKey := []byte("key3")
	success, err := store.Delete(deleteKey)
	if err != nil {
		t.Fatalf("failed to delete key %s: %v", deleteKey, err)
	}
	if !success {
		t.Fatalf("should be able to delete the key %s", deleteKey)
	}

	store.Close()

	newStore, err := NewStore(tempDir)
	if err != nil {
		t.Fatalf("failed to create a new store from temp dir: %v", err)
	}
	defer newStore.Close()

	for _, ev := range expected {
		t.Run("Get_after_delete_recovery"+string(ev.key), func(t *testing.T) {
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

	t.Run("Get_afrer_delete_recovery"+string(deleteKey), func(t *testing.T) {
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
