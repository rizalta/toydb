// Package db
package db

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/rizalta/toydb/catalog"
	"github.com/rizalta/toydb/index"
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
	keyPrefix := make([]byte, 4)
	binary.LittleEndian.PutUint32(keyPrefix, tableID)

	var keySuffix []byte
	switch pk := primaryKey.(type) {
	case int64:
		keySuffix = make([]byte, 8)
		binary.LittleEndian.PutUint64(keySuffix, uint64(pk))
	case float64:
		keySuffix = make([]byte, 8)
		binary.LittleEndian.PutUint64(keySuffix, math.Float64bits(pk))
	case string:
		keySuffix = []byte(pk)
	default:
		return nil, ErrInvalidPrimaryKey
	}

	key := append(keyPrefix, keySuffix...)
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

func (db *Database) Delete(tableName string, primaryKey tuple.Value) error {
	schema, err := db.catalog.GetTable(tableName)
	if err != nil {
		return err
	}

	key, err := createKey(schema.ID, primaryKey)
	if err != nil {
		return err
	}

	if _, err := db.store.Delete(key); !errors.Is(err, index.ErrKeyNotFound) {
		return err
	}
	return nil
}

func (db *Database) CreateTable(tableName string, columns []catalog.Column) (*catalog.Schema, error) {
	return db.catalog.CreateTable(tableName, columns)
}

func (db *Database) Close() error {
	if err := db.store.Close(); err != nil {
		return err
	}
	return db.catalog.Close()
}
