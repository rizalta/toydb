package tuple

import (
	"errors"
	"reflect"
	"testing"

	"github.com/rizalta/toydb/catalog"
)

func TestSerializeDeserialize(t *testing.T) {
	columns := []catalog.Column{
		{
			Name:         "name",
			Type:         catalog.TypeVarChar,
			IsPrimaryKey: true,
			IsNotNull:    true,
		},
		{
			Name: "age",
			Type: catalog.TypeInt,
		},
		{
			Name: "employed",
			Type: catalog.TypeBoolean,
		},
		{
			Name: "profile_picture",
			Type: catalog.TypeBlob,
		},
		{
			Name: "salary",
			Type: catalog.TypeFloat,
		},
	}
	schema := &catalog.Schema{
		ID:              1,
		Name:            "user",
		Columns:         columns,
		PrimaryKeyIndex: 0,
	}

	tests := []struct {
		name         string
		tuple        Tuple
		serializeErr error
	}{
		{
			name: "Test_normal_values",
			tuple: Tuple{
				"username",
				int64(26),
				true,
				[]byte{10, 2, 3, 44, 5},
				41000.00,
			},
		},
		{
			name: "Test_zero_values",
			tuple: Tuple{
				"",
				int64(0),
				false,
				[]byte{},
				float64(0.0),
			},
		},
		{
			name: "Test_special_characters",
			tuple: Tuple{
				"username&#(@",
				int64(-26),
				true,
				[]byte{1, 2, 4},
				float64(12e33),
			},
		},
		{
			name: "Test_invalid_num_values",
			tuple: Tuple{
				"username&#(@",
				int64(-26),
				true,
				float64(12e33),
			},
			serializeErr: ErrColumnCountMismatch,
		},
		{
			name: "Test_type_mismatch_int",
			tuple: Tuple{
				"username",
				float64(22),
				true,
				[]byte{10, 2, 3, 44},
				float64(32.01),
			},
			serializeErr: ErrTypeMismatch,
		},
		{
			name: "Test_type_mismatch_string",
			tuple: Tuple{
				1,
				22,
				true,
				[]byte{10, 2, 3, 44},
				float64(32.01),
			},
			serializeErr: ErrTypeMismatch,
		},
		{
			name: "Test_type_mismatch_bool",
			tuple: Tuple{
				"username",
				22,
				"true",
				[]byte{10, 2, 3, 44},
				float64(32.01),
			},
			serializeErr: ErrTypeMismatch,
		},
		{
			name: "Test_type_mismatch_float",
			tuple: Tuple{
				"username",
				22,
				true,
				[]byte{10, 2, 3, 44},
				-11,
			},
			serializeErr: ErrTypeMismatch,
		},
		{
			name: "Test_type_mismatch_blob",
			tuple: Tuple{
				"username",
				22,
				true,
				"not blob",
				float64(10),
			},
			serializeErr: ErrTypeMismatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serializedData, err := Serialize(tt.tuple, schema)
			if tt.serializeErr == nil {
				if err != nil {
					t.Fatalf("failed to serialize tuple: %v", err)
				}

				deserilizedTuple, err := Deserialize(serializedData, schema)
				if err != nil {
					t.Fatalf("failed to deserilize data: %v", err)
				}

				if !reflect.DeepEqual(tt.tuple, deserilizedTuple) {
					t.Errorf("expected tuple %v, but got %v", tt.tuple, deserilizedTuple)
				}
			} else {
				if !errors.Is(err, tt.serializeErr) {
					t.Errorf("expected error %v when serialization, got %v", tt.serializeErr, err)
				}
			}
		})
	}
}

func TestDeserializeCorruptData(t *testing.T) {
	schema := &catalog.Schema{
		ID:   1,
		Name: "testing",
		Columns: []catalog.Column{
			{Name: "col_int", Type: catalog.TypeInt, IsPrimaryKey: true, IsNotNull: true},
			{Name: "col_varchar", Type: catalog.TypeVarChar},
			{Name: "col_bool", Type: catalog.TypeBoolean},
			{Name: "col_blob", Type: catalog.TypeBlob},
			{Name: "col_float", Type: catalog.TypeFloat},
		},
	}

	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "Data too short",
			data: []byte{
				5,
			},
		},
		{
			name: "Header too short",
			data: []byte{
				5, 0,
				8, 0,
				11, 0,
				12, 0,
			},
		},
		{
			name: "Length mismatch",
			data: []byte{
				5, 0,
				8, 0,
				11, 0,
				12, 0,
				15, 0,
				23, 0,
				123, 0, 0, 0, 0, 0, 0, 0,
				97, 98, 99,
				1,
				1, 2, 3,
				154, 153, 153, 153, 153, 153, 64,
			},
		},
		{
			name: "Header mismatch",
			data: []byte{
				5, 0,
				10, 0,
				11, 0,
				12, 0,
				15, 0,
				23, 0,
				123, 0, 0, 0, 0, 0, 0, 0,
				97, 98, 99,
				1,
				1, 2, 3,
				154, 153, 153, 153, 153, 153, 40, 64,
			},
		},
		{
			name: "Offset wrong order",
			data: []byte{
				5, 0,
				8, 0,
				12, 0,
				11, 0,
				23, 0,
				15, 0,
				123, 0, 0, 0, 0, 0, 0, 0,
				97, 98, 99,
				1,
				1, 2, 3,
				154, 153, 153, 153, 153, 153, 40, 64,
			},
		},
		{
			name: "Num values mismatch",
			data: []byte{
				3, 0,
				8, 0,
				11, 0,
				12, 0,
				15, 0,
				23, 0,
				123, 0, 0, 0, 0, 0, 0, 0,
				97, 98, 99,
				1,
				1, 2, 3,
				154, 153, 153, 153, 153, 153, 40, 64,
			},
		},
		{
			name: "Fixed size type length wrong",
			data: []byte{
				5, 0,
				4, 0,
				11, 0,
				12, 0,
				15, 0,
				23, 0,
				123, 0, 0, 0, 0, 0, 0, 0,
				97, 98, 99,
				1,
				1, 2, 3,
				154, 153, 153, 153, 153, 153, 40, 64,
			},
		},
		{
			name: "Corrupt boolean",
			data: []byte{
				5, 0,
				8, 0,
				11, 0,
				12, 0,
				15, 0,
				23, 0,
				123, 0, 0, 0, 0, 0, 0, 0,
				97, 98, 99,
				3,
				1, 2, 3,
				154, 153, 153, 153, 153, 153, 40, 64,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Deserialize(tt.data, schema)
			if !errors.Is(err, ErrCorruptData) {
				t.Errorf("expected error %v, got %v", ErrCorruptData, err)
			}
		})
	}
}
