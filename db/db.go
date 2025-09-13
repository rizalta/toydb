// Package db
package db

import (
	"fmt"

	"github.com/rizalta/toydb/catalog"
	"github.com/rizalta/toydb/storage"
	"github.com/rizalta/toydb/tuple"
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

func createKey(tableID uint32, primaryKey tuple.Value) []byte {
	return fmt.Appendf(nil, "%d:%v", tableID, primaryKey)
}

func (db *Database) Insert(tableName string, row tuple.Tuple) error {
	schema, err := db.catalog.GetTable(tableName)
	if err != nil {
		return err
	}

	data, err := tuple.Serialize(row, schema)
	if err != nil {
		return err
	}

	key := createKey(schema.ID, row[schema.PrimaryKeyIndex])
	return db.store.Put(key, data)
}

func (db *Database) Close() error {
	return db.store.Close()
}
