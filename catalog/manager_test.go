package catalog

import (
	"errors"
	"slices"
	"testing"

	"github.com/rizalta/toydb/storage"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()

	tempDir := t.TempDir()
	store, err := storage.NewStore(tempDir)
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}

	manager, err := NewManager(store)
	if err != nil {
		t.Fatalf("failed to initialize manager: %v", err)
	}

	return manager
}

func TestCreateTable(t *testing.T) {
	manager := newTestManager(t)
	defer manager.store.Close()

	tableName := "table1"
	columns := []Column{
		{
			Name:         "attr1",
			Type:         TypeInt,
			IsPrimaryKey: true,
			IsNotNull:    true,
		},
		{
			Name: "attr2",
			Type: TypeVarChar,
		},
	}

	t.Run("Test_create_table", func(t *testing.T) {
		schema, err := manager.CreateTable(tableName, columns)
		if err != nil {
			t.Fatalf("failed to create table %s: %v", tableName, err)
		}

		if manager.meta.NextID != 2 {
			t.Errorf("expected next page id to be 2, but got %d", manager.meta.NextID)
		}

		if schema.ID != 1 {
			t.Errorf("expected table id to be 1, but got %d", schema.ID)
		}

		if schema.Name != tableName {
			t.Errorf("expected table name %s, but got %s", tableName, schema.Name)
		}

		if !slices.Equal(schema.Columns, columns) {
			t.Errorf("expected columns %v, but got %v", columns, schema.Columns)
		}
	})
	t.Run("Test_create_table_duplicate", func(t *testing.T) {
		_, err := manager.CreateTable(tableName, columns)
		if !errors.Is(err, ErrAlreadyExists) {
			t.Fatalf("expected error %v, but got %v", ErrAlreadyExists, err)
		}
	})
}

func TestGetTable(t *testing.T) {
	manager := newTestManager(t)
	defer manager.store.Close()

	tableName := "table1"
	columns := []Column{
		{
			Name:         "attr1",
			Type:         TypeInt,
			IsPrimaryKey: true,
			IsNotNull:    true,
		},
		{
			Name: "attr2",
			Type: TypeVarChar,
		},
	}

	_, err := manager.CreateTable(tableName, columns)
	if err != nil {
		t.Fatalf("failed to create table %s: %v", tableName, err)
	}

	schema, err := manager.GetTable(tableName)
	if err != nil {
		t.Fatalf("failed to get %s: %v", tableName, err)
	}
	if schema.ID != 1 {
		t.Errorf("expected table id to be 1, but got %d", schema.ID)
	}

	if schema.Name != tableName {
		t.Errorf("expected table name %s, but got %s", tableName, schema.Name)
	}

	if !slices.Equal(schema.Columns, columns) {
		t.Errorf("expected columns %v, but got %v", columns, schema.Columns)
	}
}

func TestGetTablePersist(t *testing.T) {
	tempDir := t.TempDir()
	store1, _ := storage.NewStore(tempDir)
	manager1, err := NewManager(store1)
	if err != nil {
		t.Fatalf("failed to initialize manager: %v", err)
	}

	tableName1 := "table1"
	columns1 := []Column{
		{
			Name:         "attr1",
			Type:         TypeInt,
			IsPrimaryKey: true,
			IsNotNull:    true,
		},
		{
			Name: "attr2",
			Type: TypeVarChar,
		},
	}

	tableName2 := "table2"
	columns2 := []Column{
		{
			Name:         "attr1",
			Type:         TypeInt,
			IsPrimaryKey: true,
			IsNotNull:    true,
		},
		{
			Name: "attr2",
			Type: TypeVarChar,
		},
	}

	_, err = manager1.CreateTable(tableName1, columns1)
	if err != nil {
		t.Fatalf("failed to create table %s: %v", tableName1, err)
	}

	_, err = manager1.CreateTable(tableName2, columns2)
	if err != nil {
		t.Fatalf("failed to create table %s: %v", tableName2, err)
	}
	if manager1.meta.NextID != 3 {
		t.Errorf("expected next id to be 3, but got %d", manager1.meta.NextID)
	}

	store1.Close()

	store2, _ := storage.NewStore(tempDir)
	defer store2.Close()

	manager2, err := NewManager(store2)
	if err != nil {
		t.Fatalf("failed to initialize manager: %v", err)
	}
	if manager2.meta.NextID != 3 {
		t.Errorf("expected next id to be 3, but got %d", manager2.meta.NextID)
	}

	schema1, err := manager2.GetTable(tableName1)
	if err != nil {
		t.Fatalf("failed to get %s: %v", tableName1, err)
	}
	if schema1.ID != 1 {
		t.Errorf("expected table id to be 1, but got %d", schema1.ID)
	}

	if schema1.Name != tableName1 {
		t.Errorf("expected table name %s, but got %s", tableName1, schema1.Name)
	}

	if !slices.Equal(schema1.Columns, columns1) {
		t.Errorf("expected columns %v, but got %v", columns1, schema1.Columns)
	}

	schema2, err := manager2.GetTable(tableName1)
	if err != nil {
		t.Fatalf("failed to get %s: %v", tableName2, err)
	}
	if schema2.ID != 1 {
		t.Errorf("expected table id to be 1, but got %d", schema2.ID)
	}

	if schema2.Name != tableName1 {
		t.Errorf("expected table name %s, but got %s", tableName2, schema2.Name)
	}

	if !slices.Equal(schema2.Columns, columns1) {
		t.Errorf("expected columns %v, but got %v", columns2, schema2.Columns)
	}
}

func TestCreateTable_NoPrimaryKey(t *testing.T) {
	manager := newTestManager(t)
	defer manager.store.Close()

	columns := []Column{
		{
			Name: "name",
			Type: TypeVarChar,
		},
		{
			Name: "age",
			Type: TypeInt,
		},
		{
			Name: "address",
			Type: TypeVarChar,
		},
	}

	tableName := "user"
	_, err := manager.CreateTable(tableName, columns)
	if !errors.Is(err, ErrNoPrimaryKey) {
		t.Errorf("expected error %v, but got %v", ErrNoPrimaryKey, err)
	}
}

func TestCreateTable_MultiplePrimaryKeys(t *testing.T) {
	manager := newTestManager(t)
	defer manager.store.Close()

	columns := []Column{
		{
			Name:         "name",
			Type:         TypeVarChar,
			IsPrimaryKey: true,
			IsNotNull:    true,
		},
		{
			Name: "age",
			Type: TypeInt,
		},
		{
			Name:         "address",
			Type:         TypeVarChar,
			IsPrimaryKey: true,
			IsNotNull:    true,
		},
	}

	tableName := "user"
	_, err := manager.CreateTable(tableName, columns)
	if !errors.Is(err, ErrMultiplePrimaryKeys) {
		t.Errorf("expected error %v, but got %v", ErrNoPrimaryKey, err)
	}
}

func TestCreateTable_PrimaryKeyNotNull(t *testing.T) {
	manager := newTestManager(t)
	defer manager.store.Close()

	columns := []Column{
		{
			Name:         "name",
			Type:         TypeVarChar,
			IsPrimaryKey: true,
			IsNotNull:    false,
		},
		{
			Name: "age",
			Type: TypeInt,
		},
		{
			Name: "address",
			Type: TypeVarChar,
		},
	}

	tableName := "user"
	_, err := manager.CreateTable(tableName, columns)
	if !errors.Is(err, ErrPrimaryKeyNotNull) {
		t.Errorf("expected error %v, but got %v", ErrNoPrimaryKey, err)
	}
}
