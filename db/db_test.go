package db

import (
	"errors"
	"reflect"
	"testing"

	"github.com/rizalta/toydb/catalog"
	"github.com/rizalta/toydb/index"
	"github.com/rizalta/toydb/tuple"
)

func newTestDB(t *testing.T) *Database {
	t.Helper()

	tempDir := t.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		t.Fatalf("failed to initialize test db: %v", err)
	}

	return db
}

func TestDBPutGet(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	tableName := "users"
	columns := []catalog.Column{
		{Name: "id", Type: catalog.TypeInt, IsPrimaryKey: true, IsNotNull: true},
		{Name: "username", Type: catalog.TypeVarChar, IsNotNull: true},
		{Name: "is_active", Type: catalog.TypeBoolean},
	}
	_, err := db.catalog.CreateTable(tableName, columns)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	tests := []struct {
		name string
		row  tuple.Tuple
		err  error
	}{
		{
			name: "simple insert",
			row:  tuple.Tuple{int64(1), "user1", true},
		},
		{
			name: "insert with allowed null value",
			row:  tuple.Tuple{int64(2), "user2", nil},
		},
		{
			name: "existing primary key",
			row:  tuple.Tuple{int64(1), "user3", true},
			err:  index.ErrKeyAlreadyExists,
		},
		{
			name: "primary key null",
			row:  tuple.Tuple{nil, "user3", false},
			err:  ErrInvalidPrimaryKey,
		},
		{
			name: "not null value is null",
			row:  tuple.Tuple{int64(3), nil, true},
			err:  ErrNotNULL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := db.Insert(tableName, tt.row)
			if tt.err == nil {
				if err != nil {
					t.Errorf("failed to insert row %v: %v", tt.row, err)
				}

				retrievedRow, found, err := db.Get(tableName, tt.row[0])
				if err != nil {
					t.Fatalf("failed to get for primary key %d: %v", tt.row[0], err)
				}
				if !found {
					t.Fatalf("expected true for found got false")
				}
				if !reflect.DeepEqual(retrievedRow, tt.row) {
					t.Errorf("expected row %v, got %v", tt.row, retrievedRow)
				}
			} else {
				if !errors.Is(err, tt.err) {
					t.Errorf("expected error %v, got %v", tt.err, err)
				}
			}
		})
	}

	t.Run("get non existing key", func(t *testing.T) {
		_, found, err := db.Get(tableName, int64(444))
		if err != nil {
			t.Fatalf("failed to get for non existing key: %v", err)
		}
		if found {
			t.Errorf("expected found to be false, got true")
		}
	})
}

func TestUpdate(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	tableName := "users"
	columns := []catalog.Column{
		{Name: "id", Type: catalog.TypeInt, IsPrimaryKey: true, IsNotNull: true},
		{Name: "name", Type: catalog.TypeVarChar, IsNotNull: true},
	}
	_, err := db.catalog.CreateTable(tableName, columns)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	primaryKey := int64(1)
	row1 := tuple.Tuple{primaryKey, "alice"}
	row2 := tuple.Tuple{primaryKey, "bob"}

	t.Run("Update_non_existant", func(t *testing.T) {
		err := db.Update(tableName, row1)
		if !errors.Is(err, index.ErrKeyNotFound) {
			t.Errorf("expected error %v, got %v", index.ErrKeyNotFound, err)
		}
	})

	err = db.Insert(tableName, row1)
	if err != nil {
		t.Fatalf("failed to insert initailly: %v", err)
	}

	t.Run("Update_existant", func(t *testing.T) {
		err := db.Update(tableName, row2)
		if err != nil {
			t.Fatalf("failed to update: %v", err)
		}

		retrievedRow, found, err := db.Get(tableName, primaryKey)
		if err != nil || !found {
			t.Fatalf("expected no error and found true, got %v, false", err)
		}
		if !reflect.DeepEqual(row2, retrievedRow) {
			t.Errorf("expected row %v, got %v", row2, retrievedRow)
		}
	})
}

func TestPutGetStringPrimaryKey(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	tableName := "users"
	columns := []catalog.Column{
		{Name: "id", Type: catalog.TypeInt, IsNotNull: true},
		{Name: "username", Type: catalog.TypeVarChar, IsPrimaryKey: true, IsNotNull: true},
		{Name: "is_active", Type: catalog.TypeBoolean},
	}
	_, err := db.catalog.CreateTable(tableName, columns)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	tests := []struct {
		name string
		row  tuple.Tuple
		err  error
	}{
		{
			name: "simple insert",
			row:  tuple.Tuple{int64(1), "user1", true},
		},
		{
			name: "insert with allowed null value",
			row:  tuple.Tuple{int64(2), "user2", nil},
		},
		{
			name: "existing primary key",
			row:  tuple.Tuple{int64(3), "user1", true},
			err:  index.ErrKeyAlreadyExists,
		},
		{
			name: "primary key null",
			row:  tuple.Tuple{int64(3), nil, false},
			err:  ErrInvalidPrimaryKey,
		},
		{
			name: "not null value is null",
			row:  tuple.Tuple{nil, "user3", true},
			err:  ErrNotNULL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := db.Insert(tableName, tt.row)
			if tt.err == nil {
				if err != nil {
					t.Errorf("failed to insert row %v: %v", tt.row, err)
				}

				retrievedRow, found, err := db.Get(tableName, tt.row[1])
				if err != nil {
					t.Fatalf("failed to get for primary key %d: %v", tt.row[1], err)
				}
				if !found {
					t.Fatalf("expected true for found got false")
				}
				if !reflect.DeepEqual(retrievedRow, tt.row) {
					t.Errorf("expected row %v, got %v", tt.row, retrievedRow)
				}
			} else {
				if !errors.Is(err, tt.err) {
					t.Errorf("expected error %v, got %v", tt.err, err)
				}
			}
		})
	}

	t.Run("get non existing key", func(t *testing.T) {
		_, found, err := db.Get(tableName, "user7474")
		if err != nil {
			t.Fatalf("failed to get for non existing key: %v", err)
		}
		if found {
			t.Errorf("expected found to be false, got true")
		}
	})
}

func TestDBDelete(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	tableName := "users"
	columns := []catalog.Column{
		{Name: "id", Type: catalog.TypeInt, IsPrimaryKey: true, IsNotNull: true},
		{Name: "name", Type: catalog.TypeVarChar, IsNotNull: true},
	}
	db.catalog.CreateTable(tableName, columns)

	row1 := tuple.Tuple{int64(1), "alice"}
	row2 := tuple.Tuple{int64(2), "bob"}
	pk1 := row1[0]
	pk2 := row2[0]
	db.Insert(tableName, row1)
	db.Insert(tableName, row2)

	t.Run("Verify_delete_existing_row", func(t *testing.T) {
		err := db.Delete(tableName, pk1)
		if err != nil {
			t.Fatalf("failed to delete primary key %v", pk1)
		}

		_, found, err := db.Get(tableName, pk1)
		if err != nil {
			t.Fatalf("expected error to be nil  when get deleted key, got %v", err)
		}
		if found {
			t.Errorf("expected found to be false for deleted key")
		}
		retrievedRow, found, err := db.Get(tableName, pk2)
		if !found || err != nil {
			t.Fatalf("found should be true and err is nil for %v, got %v", pk2, err)
		}
		if !reflect.DeepEqual(retrievedRow, row2) {
			t.Errorf("expected row %v, got %v", row2, retrievedRow)
		}
	})

	t.Run("Verify_delete_non_existing", func(t *testing.T) {
		nonExistantKey := int64(3)
		err := db.Delete(tableName, nonExistantKey)
		if err != nil {
			t.Fatalf("non existant key should not return error: %v", err)
		}
	})

	t.Run("Verify_delete_key_again", func(t *testing.T) {
		err := db.Delete(tableName, pk1)
		if err != nil {
			t.Fatalf("deleting key again not return error: %v", err)
		}
	})
}
