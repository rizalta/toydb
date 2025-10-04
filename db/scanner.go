package db

import (
	"encoding/binary"

	"github.com/rizalta/toydb/catalog"
	"github.com/rizalta/toydb/storage"
	"github.com/rizalta/toydb/tuple"
)

type Scanner struct {
	iterator *storage.Iterator
	schema   *catalog.Schema
}

func (db *Database) Scan(tableName string, start, end tuple.Value) (*Scanner, error) {
	schema, err := db.catalog.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	primaryKeyType := schema.Columns[schema.PrimaryKeyIndex].Type

	var startKey, endKey []byte

	if start == nil {
		startKey = make([]byte, 8)
		binary.BigEndian.PutUint32(startKey, schema.ID)
	} else {
		if !isTypeMatch(primaryKeyType, start) {
			return nil, ErrInvalidPrimaryKey
		}
		startKey, err = createKey(schema.ID, start)
		if err != nil {
			return nil, err
		}
	}

	if end == nil {
		endKey = make([]byte, 8)
		binary.BigEndian.PutUint32(endKey, schema.ID+1)
	} else {
		if !isTypeMatch(primaryKeyType, end) {
			return nil, ErrInvalidPrimaryKey
		}
		endKey, err = createKey(schema.ID, end)
		if err != nil {
			return nil, err
		}
	}

	iterator, err := db.store.NewIterator(startKey, endKey)
	if err != nil {
		return nil, err
	}

	return &Scanner{
		iterator: iterator,
		schema:   schema,
	}, nil
}

func (s *Scanner) Next() (tuple.Tuple, error) {
	_, value, err := s.iterator.Next()
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}

	return tuple.Deserialize(value, s.schema)
}
