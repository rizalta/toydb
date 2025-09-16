// Package storage
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"

	"github.com/rizalta/toydb/index"
	"github.com/rizalta/toydb/pager"
)

type Pager interface {
	WriteAtOffset(offset uint64, data []byte) error
	ReadAtOffset(offset uint64, size int) ([]byte, error)
	Close() error
}

type Index interface {
	Insert(key uint64, value uint64, insertMode index.InsertMode) error
	Search(key uint64) (uint64, error)
	Delete(key uint64) error
	Close() error
}

type Store struct {
	pager   Pager
	index   Index
	offset  uint64
	dataDir string
}

type RecordType byte

const (
	RecordTypeInsert RecordType = 0
	RecordTypeDelete RecordType = 1
)

type Record struct {
	RecordType RecordType
	Key        []byte
	Value      []byte
}

const (
	indexFile = "index.db"
	dataFile  = "data.db"
	lockFile  = "clean.lock"
)

func NewStore(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}

	dataPath := filepath.Join(dataDir, dataFile)
	dataPager, err := pager.NewPager(dataPath)
	if err != nil {
		return nil, err
	}

	indexPath := filepath.Join(dataDir, indexFile)
	indexPager, err := pager.NewPager(indexPath)
	if err != nil {
		dataPager.Close()
		return nil, err
	}

	index, err := index.NewIndex(indexPager)
	if err != nil {
		dataPager.Close()
		indexPager.Close()
		return nil, err
	}

	s := &Store{
		pager:   dataPager,
		index:   index,
		offset:  0,
		dataDir: dataDir,
	}

	lockFilePath := filepath.Join(dataDir, lockFile)
	if _, err := os.Stat(lockFilePath); err == nil {
		offset := uint64(0)
		for {
			r, err := s.readRecord(offset)
			if err != nil {
				break
			}
			offset += uint64(len(r.serialize()))
		}
		s.offset = offset
		if err := os.Remove(lockFilePath); err != nil {
			return nil, err
		}
	} else if os.IsNotExist(err) {
		if err := s.recoverIndex(); err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}

	return s, nil
}

func (s *Store) recoverIndex() error {
	offset := uint64(0)
	for {
		r, err := s.readRecord(offset)
		if err != nil {
			break
		}

		err = s.index.Insert(hashKey(r.Key), offset, index.Upsert)
		if err != nil {
			return err
		}
		offset += uint64(len(r.serialize()))
	}
	s.offset = offset
	return nil
}

func (r *Record) serialize() []byte {
	keyBytes := []byte(r.Key)

	keyLen := uint32(len(keyBytes))
	valueLen := uint32(len(r.Value))

	totalLength := 9 + len(r.Key) + len(r.Value)
	buf := make([]byte, totalLength)

	buf[0] = byte(r.RecordType)

	binary.LittleEndian.PutUint32(buf[1:5], keyLen)
	binary.LittleEndian.PutUint32(buf[5:9], valueLen)
	copy(buf[9:9+keyLen], keyBytes)
	if r.Value != nil {
		copy(buf[9+keyLen:], r.Value)
	}

	return buf
}

func deserialize(data []byte) (*Record, error) {
	if len(data) < 9 {
		return nil, fmt.Errorf("storage: record too short")
	}

	recordType := RecordType(data[0])

	keyLen := binary.LittleEndian.Uint32(data[1:5])
	valuelen := binary.LittleEndian.Uint32(data[5:9])

	if len(data) < int(9+keyLen+valuelen) {
		return nil, fmt.Errorf("storage: record data truncated")
	}

	key := data[9 : 9+keyLen]

	var value []byte
	if recordType == RecordTypeInsert && valuelen > 0 {
		value = make([]byte, valuelen)
		copy(value, data[9+keyLen:9+keyLen+valuelen])
	}

	return &Record{
		RecordType: recordType,
		Key:        key,
		Value:      value,
	}, nil
}

func hashKey(key []byte) uint64 {
	h := fnv.New64a()
	h.Write(key)
	return h.Sum64()
}

func (s *Store) Put(key []byte, value []byte) error {
	record := &Record{
		RecordType: RecordTypeInsert,
		Key:        key,
		Value:      value,
	}

	serialized := record.serialize()

	err := s.pager.WriteAtOffset(s.offset, serialized)
	if err != nil {
		return fmt.Errorf("storage: failed to write record: %v", err)
	}

	err = s.index.Insert(hashKey(key), s.offset, index.Upsert)
	if err != nil {
		return fmt.Errorf("storage: failed to index key: %v", err)
	}

	s.offset += uint64(len(serialized))

	return nil
}

func (s *Store) Update(key []byte, value []byte) error {
	record := &Record{
		RecordType: RecordTypeInsert,
		Key:        key,
		Value:      value,
	}

	serialized := record.serialize()

	err := s.pager.WriteAtOffset(s.offset, serialized)
	if err != nil {
		return fmt.Errorf("storage: failed to write record: %v", err)
	}

	err = s.index.Insert(hashKey(key), s.offset, index.UpdateOnly)
	if err != nil {
		if errors.Is(err, index.ErrKeyNotFound) {
			return err
		}
		return fmt.Errorf("storage: failed to index key: %v", err)
	}

	s.offset += uint64(len(serialized))

	return nil
}

func (s *Store) Add(key []byte, value []byte) error {
	record := &Record{
		RecordType: RecordTypeInsert,
		Key:        key,
		Value:      value,
	}

	serialized := record.serialize()

	err := s.pager.WriteAtOffset(s.offset, serialized)
	if err != nil {
		return fmt.Errorf("storage: failed to write record: %v", err)
	}

	err = s.index.Insert(hashKey(key), s.offset, index.InsertOnly)
	if err != nil {
		if errors.Is(err, index.ErrKeyAlreadyExists) {
			return err
		}
		return fmt.Errorf("storage: failed to index key: %v", err)
	}

	s.offset += uint64(len(serialized))

	return nil
}

func (s *Store) readRecord(offset uint64) (*Record, error) {
	headerData, err := s.pager.ReadAtOffset(offset, 9)
	if err != nil {
		return nil, err
	}

	keyLen := binary.LittleEndian.Uint32(headerData[1:5])
	valuelen := binary.LittleEndian.Uint32(headerData[5:9])

	remaining := int(keyLen + valuelen)
	remainingData, err := s.pager.ReadAtOffset(offset+9, remaining)
	if err != nil {
		return nil, err
	}

	data := make([]byte, 9+remaining)
	copy(data, headerData)
	copy(data[9:], remainingData)

	return deserialize(data)
}

func (s *Store) Get(key []byte) ([]byte, bool, error) {
	offset, err := s.index.Search(hashKey(key))
	if err != nil {
		if errors.Is(err, index.ErrKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}

	record, err := s.readRecord(offset)
	if err != nil {
		return nil, false, err
	}

	if record.RecordType == RecordTypeDelete {
		return nil, false, nil
	}

	return record.Value, true, nil
}

func (s *Store) Delete(key []byte) (bool, error) {
	offset, err := s.index.Search(hashKey(key))
	if err != nil {
		if errors.Is(err, index.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}

	record, err := s.readRecord(offset)
	if err != nil {
		return false, err
	}

	if record.RecordType == RecordTypeDelete {
		return false, nil
	}

	record = &Record{
		RecordType: RecordTypeDelete,
		Key:        key,
		Value:      nil,
	}

	serialized := record.serialize()
	err = s.pager.WriteAtOffset(s.offset, serialized)
	if err != nil {
		return false, fmt.Errorf("storage: failed to write tombstone: %v", err)
	}

	err = s.index.Insert(hashKey(key), s.offset, index.Upsert)
	if err != nil {
		return false, fmt.Errorf("storage: failed to index key: %v", err)
	}
	s.offset += uint64(len(serialized))

	return true, nil
}

func (s *Store) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.pager.Close(); err != nil {
		return err
	}
	lockFilePath := filepath.Join(s.dataDir, lockFile)
	file, err := os.Create(lockFilePath)
	if err != nil {
		return err
	}
	return file.Close()
}
