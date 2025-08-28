// Package storage
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/rizalta/toydb/index"
)

type Pager interface {
	WriteAtOffset(offset uint64, data []byte) error
	ReadAtOffset(offset uint64, size int) ([]byte, error)
	GetSize() (uint64, error)
	Close() error
}

type Index interface {
	Insert(key uint64, value uint64)
	Search(key uint64) (uint64, error)
	Delete(key uint64) error
}

type Store struct {
	pager  Pager
	index  Index
	offset uint64
}

type RecordType byte

const (
	RecordTypeInsert RecordType = 0
	RecordTypeDelete RecordType = 1
)

type Record struct {
	RecordType RecordType
	Key        string
	Value      []byte
}

func NewStore(pager Pager, index Index) *Store {
	s := &Store{
		pager:  pager,
		index:  index,
		offset: 0,
	}

	s.Recovery()

	return s
}

func (s *Store) Recovery() {
	offset := uint64(0)
	for {
		r, err := s.readRecord(offset)
		if err != nil {
			break
		}

		s.index.Insert(hashKey(r.Key), offset)
		offset += uint64(len(r.serialize()))
	}
	s.offset = offset
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

	key := string(data[9 : 9+keyLen])

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

func hashKey(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

func (s *Store) Put(key string, value []byte) error {
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

	s.index.Insert(hashKey(key), s.offset)

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

func (s *Store) Get(key string) ([]byte, bool, error) {
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

func (s *Store) Delete(key string) (bool, error) {
	if _, err := s.index.Search(hashKey(key)); err != nil {
		if errors.Is(err, index.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}

	record := &Record{
		RecordType: RecordTypeDelete,
		Key:        key,
		Value:      nil,
	}

	serialized := record.serialize()
	err := s.pager.WriteAtOffset(s.offset, serialized)
	if err != nil {
		return false, fmt.Errorf("storage: failed to write tombstone: %v", err)
	}

	s.index.Insert(hashKey(key), s.offset)
	s.offset += uint64(len(serialized))

	return true, nil
}
