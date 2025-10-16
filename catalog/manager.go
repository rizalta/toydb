package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

var (
	ErrAlreadyExists         = errors.New("catalog: table already exists")
	ErrNoPrimaryKey          = errors.New("catalog: no primary key")
	ErrMultiplePrimaryKeys   = errors.New("catalog: multiple primary keys")
	ErrPrimaryKeyNotNull     = errors.New("catalog: primary key should be not null")
	ErrUnsupportedPrimaryKey = errors.New("catalog: unsupported type for primary key")
	ErrDuplicateColumnName   = errors.New("catalog: duplicate column name")
	ErrIndexAlreadyExists    = errors.New("catalog: index already exists")
	ErrIndexColumnNotFound   = errors.New("catalog: column not found to create index")
)

var (
	metaKey          = []byte("catalog:__schema__")
	primaryIndexName = "PRIMARY"
)

type Store interface {
	Get(key []byte) ([]byte, bool, error)
	Put(key []byte, value []byte) error
	Close() error
}

type Manager struct {
	store Store
	meta  *ManagerMeta
}

type ManagerMeta struct {
	NextID      uint32 `json:"next_id"`
	NextIndexID uint32 `json:"next_index_id"`
}

func NewManager(store Store) (*Manager, error) {
	m := &Manager{
		store: store,
		meta:  &ManagerMeta{NextID: 1, NextIndexID: 1},
	}

	var meta ManagerMeta
	metaBytes, found, err := m.store.Get(metaKey)
	if err != nil {
		return nil, err
	}
	if found {
		if err := json.Unmarshal(metaBytes, &meta); err != nil {
			return nil, err
		}
		m.meta = &meta
	}

	return m, nil
}

func (m *Manager) updateMeta() error {
	metaBytes, err := json.Marshal(m.meta)
	if err != nil {
		return err
	}

	return m.store.Put(metaKey, metaBytes)
}

func (m *Manager) updateSchema(schema *Schema) error {
	schemaKey := []byte("table:" + schema.Name)
	schemaBytes, err := json.Marshal(schema)
	if err != nil {
		return err
	}

	return m.store.Put(schemaKey, schemaBytes)
}

func (m *Manager) CreateTable(name string, columns []Column) (*Schema, error) {
	schemaKey := []byte("table:" + name)

	columnNames := make(map[string]struct{})
	for _, c := range columns {
		if _, exists := columnNames[c.Name]; exists {
			return nil, ErrDuplicateColumnName
		}
		columnNames[c.Name] = struct{}{}
	}

	primaryKeyCols := slices.Collect(func(yield func(i int) bool) {
		for i, c := range columns {
			if c.IsPrimaryKey {
				if !yield(i) {
					return
				}
			}
		}
	})

	if len(primaryKeyCols) == 0 {
		return nil, ErrNoPrimaryKey
	} else if len(primaryKeyCols) > 1 {
		return nil, ErrMultiplePrimaryKeys
	}

	primaryKeyIndex := primaryKeyCols[0]
	primaryKeyColumn := columns[primaryKeyIndex]

	switch primaryKeyColumn.Type {
	case TypeInt, TypeVarChar, TypeFloat:
	default:
		return nil, ErrUnsupportedPrimaryKey
	}

	if !primaryKeyColumn.IsNotNull {
		return nil, ErrPrimaryKeyNotNull
	}

	if _, found, err := m.store.Get(schemaKey); err != nil {
		return nil, err
	} else if found {
		return nil, ErrAlreadyExists
	}

	schema := &Schema{
		ID:              m.meta.NextID,
		Name:            name,
		Columns:         columns,
		PrimaryKeyIndex: primaryKeyIndex,
	}
	primaryKeyColName := schema.Columns[primaryKeyIndex].Name
	schema.Indexes = []*IndexInfo{
		{
			ID:      0,
			Name:    primaryIndexName,
			Columns: []string{primaryKeyColName},
		},
	}

	if err := m.updateSchema(schema); err != nil {
		return nil, err
	}

	m.meta.NextID++
	if err := m.updateMeta(); err != nil {
		return nil, err
	}

	return schema, nil
}

func (m *Manager) GetTable(name string) (*Schema, error) {
	schemaKey := []byte("table:" + name)

	schemaBytes, found, err := m.store.Get(schemaKey)
	if err != nil {
		return nil, fmt.Errorf("catalog: failed to retrieve schema: %w", err)
	}
	if !found {
		return nil, fmt.Errorf("catalog: table %s not found", name)
	}

	var schema Schema
	if err := json.Unmarshal(schemaBytes, &schema); err != nil {
		return nil, fmt.Errorf("catalog: failed to deserialize schema: %w", err)
	}

	return &schema, nil
}

func (m *Manager) CreateIndex(tableName, indexName string, columnNames []string) (*IndexInfo, error) {
	schema, err := m.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	for _, idx := range schema.Indexes {
		if idx.Name == indexName || slices.Equal(idx.Columns, columnNames) {
			return nil, ErrIndexAlreadyExists
		}
	}

	schemaColNames := make(map[string]struct{})
	for _, c := range schema.Columns {
		schemaColNames[c.Name] = struct{}{}
	}
	for _, c := range columnNames {
		if _, exists := schemaColNames[c]; !exists {
			return nil, ErrIndexColumnNotFound
		}
	}

	newIndex := &IndexInfo{
		ID:      m.meta.NextIndexID,
		Name:    indexName,
		Columns: columnNames,
	}
	schema.Indexes = append(schema.Indexes, newIndex)

	if err := m.updateSchema(schema); err != nil {
		return nil, err
	}

	m.meta.NextIndexID++
	if err := m.updateMeta(); err != nil {
		return nil, err
	}

	return newIndex, nil
}

func (m *Manager) Close() error {
	return m.store.Close()
}
