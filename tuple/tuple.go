// Package tuple
package tuple

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/rizalta/toydb/catalog"
)

type Value any

type Tuple []Value

var (
	ErrColumnCountMismatch = errors.New("tuple: number of values mismatch with schema column count")
	ErrTypeMismatch        = errors.New("tuple: value type mismatch with schema type")
	ErrCorruptData         = errors.New("tuple: data is corrupt or malformed")
)

func Serialize(tuple Tuple, schema *catalog.Schema) ([]byte, error) {
	numValues := len(tuple)
	if numValues != len(schema.Columns) {
		return nil, ErrColumnCountMismatch
	}

	encodedValues := make([][]byte, numValues)
	dataSize := 0
	for i, value := range tuple {
		colType := schema.Columns[i].Type
		var encoded []byte

		switch colType {
		case catalog.TypeInt:
			val, ok := value.(int64)
			if !ok {
				return nil, ErrTypeMismatch
			}
			encoded = make([]byte, 8)
			binary.LittleEndian.PutUint64(encoded, uint64(val))
		case catalog.TypeVarChar:
			val, ok := value.(string)
			if !ok {
				return nil, ErrTypeMismatch
			}
			encoded = []byte(val)
		case catalog.TypeBoolean:
			val, ok := value.(bool)
			if !ok {
				return nil, ErrTypeMismatch
			}
			encoded = make([]byte, 1)
			if val {
				encoded[0] = 1
			} else {
				encoded[0] = 0
			}
		case catalog.TypeBlob:
			val, ok := value.([]byte)
			if !ok {
				return nil, ErrTypeMismatch
			}
			encoded = val
		case catalog.TypeFloat:
			val, ok := value.(float64)
			if !ok {
				return nil, ErrTypeMismatch
			}
			encoded = make([]byte, 8)
			binary.LittleEndian.PutUint64(encoded, math.Float64bits(val))
		default:
			return nil, ErrTypeMismatch
		}

		dataSize += len(encoded)
		encodedValues[i] = encoded
	}

	headerSize := 2 + (2 * numValues)
	totalSize := headerSize + dataSize
	result := make([]byte, totalSize)

	binary.LittleEndian.PutUint16(result[0:], uint16(numValues))
	currentOffset := 0
	for i, valBytes := range encodedValues {
		currentOffset += len(valBytes)
		offsetPosition := 2 + (2 * i)
		binary.LittleEndian.PutUint16(result[offsetPosition:], uint16(currentOffset))
	}

	dataOffset := headerSize
	for _, valBytes := range encodedValues {
		copy(result[dataOffset:], valBytes)
		dataOffset += len(valBytes)
	}

	return result, nil
}

func Deserialize(data []byte, schema *catalog.Schema) (Tuple, error) {
	if len(data) < 2 {
		return nil, ErrCorruptData
	}

	numValues := int(binary.LittleEndian.Uint16(data[0:]))
	if numValues != len(schema.Columns) {
		return nil, ErrCorruptData
	}

	headerSize := 2 + (2 * numValues)
	if len(data) < headerSize {
		return nil, ErrCorruptData
	}

	offsets := make([]uint16, numValues)
	for i := range numValues {
		offsetPosition := 2 + (2 * i)
		offsets[i] = binary.LittleEndian.Uint16(data[offsetPosition:])
	}

	totalSize := headerSize + int(offsets[numValues-1])
	if len(data) != totalSize {
		return nil, ErrCorruptData
	}

	tuple := make(Tuple, numValues)
	dataSection := data[headerSize:]
	dataSize := uint16(len(dataSection))
	starPos := uint16(0)

	for i, endPos := range offsets {
		if starPos > endPos || endPos > dataSize {
			return nil, ErrCorruptData
		}

		valueBytes := dataSection[starPos:endPos]
		colType := schema.Columns[i].Type

		var value Value
		switch colType {
		case catalog.TypeInt:
			if len(valueBytes) != 8 {
				return nil, ErrCorruptData
			}
			value = int64(binary.LittleEndian.Uint64(valueBytes))
		case catalog.TypeVarChar:
			value = string(valueBytes)
		case catalog.TypeBoolean:
			if len(valueBytes) != 1 {
				return nil, ErrCorruptData
			}
			switch valueBytes[0] {
			case 1:
				value = true
			case 0:
				value = false
			default:
				return nil, ErrCorruptData
			}
		case catalog.TypeBlob:
			value = valueBytes
		case catalog.TypeFloat:
			if len(valueBytes) != 8 {
				return nil, ErrCorruptData
			}
			value = math.Float64frombits(binary.LittleEndian.Uint64(valueBytes))
		default:
			return nil, ErrCorruptData
		}

		tuple[i] = value
		starPos = endPos
	}

	return tuple, nil
}
