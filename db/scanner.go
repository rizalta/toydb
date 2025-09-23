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

func (db *Database) Scan(tableName string) (*Scanner, error) {
	schema, err := db.catalog.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	iterator, err := db.store.NewIterator()
	if err != nil {
		return nil, err
	}

	return &Scanner{
		iterator: iterator,
		schema:   schema,
	}, nil
}

func (s *Scanner) Next() (tuple.Tuple, error) {
	for {
		key, value, err := s.iterator.Next()
		if err != nil {
			return nil, err
		}
		if key == nil {
			return nil, nil
		}

		if len(key) < 4 {
			continue
		}
		tableID := binary.LittleEndian.Uint32(key[0:4])
		if tableID == s.schema.ID {
			row, err := tuple.Deserialize(value, s.schema)
			if err != nil {
				return nil, err
			}
			return row, nil
		}
		if tableID > s.schema.ID {
			return nil, nil
		}
	}
}
