package storage

type Cursor interface {
	Next() ([]byte, uint64, error)
}

type Iterator struct {
	store  *Store
	cursor Cursor
}

func (s *Store) NewIterator() (*Iterator, error) {
	cursor, err := s.index.NewCursor()
	if err != nil {
		return nil, err
	}

	return &Iterator{
		store:  s,
		cursor: cursor,
	}, nil
}

func (it *Iterator) Next() ([]byte, []byte, error) {
	key, offset, err := it.cursor.Next()
	if err != nil {
		return nil, nil, err
	}

	if key == nil {
		return nil, nil, nil
	}

	record, err := it.store.readRecord(offset)
	if err != nil {
		return nil, nil, err
	}

	return key, record.Value, nil
}
