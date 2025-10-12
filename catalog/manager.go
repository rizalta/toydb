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
)

var metaKey = []byte("catalog:__schema__")

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
	NextID uint32 `json:"next_id"`
}

func NewManager(store Store) (*Manager, error) {
	m := &Manager{
		store: store,
		meta:  &ManagerMeta{NextID: 1},
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

func (m *Manager) updateNextID() error {
	metaBytes, err := json.Marshal(m.meta)
	if err != nil {
		return err
	}

	return m.store.Put(metaKey, metaBytes)
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

	schemaBytes, err := json.Marshal(schema)
	if err != nil {
		return nil, err
	}

	if err := m.store.Put(schemaKey, schemaBytes); err != nil {
		return nil, err
	}

	m.meta.NextID++
	if err := m.updateNextID(); err != nil {
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

func (m *Manager) Close() error {
	return m.store.Close()
}
