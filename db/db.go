// Package db
package db

import (
	"encoding/binary"
	"errors"

	"github.com/rizalta/toydb/catalog"
	"github.com/rizalta/toydb/storage"
	"github.com/rizalta/toydb/tuple"
)

var (
	ErrInvalidPrimaryKey   = errors.New("db: primary key should be int64")
	ErrColumnCountMismatch = errors.New("db: number of values mismatch with schema column count")
	ErrNotNULL             = errors.New("db: value cannot be NULL")
)

type Database struct {
	store   *storage.Store
	catalog *catalog.Manager
}

func NewDatabase(dirPath string) (*Database, error) {
	store, err := storage.NewStore(dirPath)
	if err != nil {
		return nil, err
	}
	catalog, err := catalog.NewManager(store)
	if err != nil {
		store.Close()
		return nil, err
	}

	db := &Database{
		store:   store,
		catalog: catalog,
	}

	return db, nil
}

func createKey(tableID uint32, primaryKey tuple.Value) ([]byte, error) {
	pkVal, ok := primaryKey.(int64)
	if !ok {
		return nil, ErrInvalidPrimaryKey
	}

	key := make([]byte, 12)
	binary.LittleEndian.PutUint32(key[0:], tableID)
	binary.LittleEndian.PutUint64(key[4:], uint64(pkVal))

	return key, nil
}

func (db *Database) Insert(tableName string, row tuple.Tuple) error {
	schema, err := db.catalog.GetTable(tableName)
	if err != nil {
		return err
	}

	if len(row) != len(schema.Columns) {
		return ErrColumnCountMismatch
	}

	for i, column := range schema.Columns {
		if column.IsNotNull && row[i] == nil {
			if column.IsPrimaryKey {
				return ErrInvalidPrimaryKey
			}
			return ErrNotNULL
		}
	}

	data, err := tuple.Serialize(row, schema)
	if err != nil {
		return err
	}

	key, err := createKey(schema.ID, row[schema.PrimaryKeyIndex])
	if err != nil {
		return err
	}

	return db.store.Add(key, data)
}

func (db *Database) Get(tableName string, primaryKey tuple.Value) (tuple.Tuple, bool, error) {
	schema, err := db.catalog.GetTable(tableName)
	if err != nil {
		return nil, false, err
	}

	key, err := createKey(schema.ID, primaryKey)
	if err != nil {
		return nil, false, err
	}

	valueBytes, found, err := db.store.Get(key)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, found, nil
	}

	row, err := tuple.Deserialize(valueBytes, schema)
	if err != nil {
		return nil, false, err
	}

	return row, true, nil
}

func (db *Database) Update(tableName string, row tuple.Tuple) error {
	schema, err := db.catalog.GetTable(tableName)
	if err != nil {
		return err
	}

	if len(row) != len(schema.Columns) {
		return ErrColumnCountMismatch
	}

	for i, column := range schema.Columns {
		if column.IsNotNull && row[i] == nil {
			if column.IsPrimaryKey {
				return ErrInvalidPrimaryKey
			}
			return ErrNotNULL
		}
	}

	valueBytes, err := tuple.Serialize(row, schema)
	if err != nil {
		return err
	}

	key, err := createKey(schema.ID, row[schema.PrimaryKeyIndex])
	if err != nil {
		return err
	}

	return db.store.Update(key, valueBytes)
}

func (db *Database) Close() error {
	return db.store.Close()
}
