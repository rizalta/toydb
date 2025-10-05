package db

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/rizalta/toydb/catalog"
	"github.com/rizalta/toydb/tuple"
)

func TestDB_Scanner(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	t.Log("Creating 4 tables, one is empty")
	columns := make([][]catalog.Column, 3)

	columns[0] = []catalog.Column{
		{Name: "id", Type: catalog.TypeInt, IsPrimaryKey: true, IsNotNull: true},
		{Name: "name", Type: catalog.TypeVarChar, IsNotNull: true},
	}

	columns[1] = []catalog.Column{
		{Name: "id", Type: catalog.TypeInt, IsNotNull: true},
		{Name: "name", Type: catalog.TypeVarChar, IsPrimaryKey: true, IsNotNull: true},
	}

	columns[2] = []catalog.Column{
		{Name: "id", Type: catalog.TypeInt, IsPrimaryKey: true, IsNotNull: true},
		{Name: "task", Type: catalog.TypeVarChar, IsNotNull: true},
	}

	tableName := func(i int) string { return fmt.Sprintf("table_%d", i) }

	fullRows := make([][]tuple.Tuple, 3)
	for i := range 3 {
		_, err := db.CreateTable(tableName(i), columns[i])
		if err != nil {
			t.Fatalf("failed to create table %s: %v", tableName(i), err)
		}

		for j := range (i + 1) * 1000 {
			row := tuple.Tuple{int64(j), fmt.Sprintf("name_%d_%05d", i, j)}
			err := db.Insert(tableName(i), row)
			if err != nil {
				t.Fatalf("error inserting row %v into table %s: %v", row, tableName(i), err)
			}
			fullRows[i] = append(fullRows[i], row)
		}
	}

	_, err := db.CreateTable(tableName(3), columns[0])
	if err != nil {
		t.Fatalf("failed to create table %s: %v", tableName(3), err)
	}

	tests := []struct {
		name         string
		tableName    string
		startKey     tuple.Value
		endKey       tuple.Value
		expectedRows []tuple.Tuple
	}{
		{
			name:         "full scan table 0",
			tableName:    tableName(0),
			startKey:     nil,
			endKey:       nil,
			expectedRows: fullRows[0],
		},
		{
			name:         "full scan table 1",
			tableName:    tableName(1),
			startKey:     nil,
			endKey:       nil,
			expectedRows: fullRows[1],
		},
		{
			name:         "full scan table 2",
			tableName:    tableName(2),
			startKey:     nil,
			endKey:       nil,
			expectedRows: fullRows[2],
		},
		{
			name:         "bound scan",
			tableName:    tableName(1),
			startKey:     "name_1_00456",
			endKey:       "name_1_01679",
			expectedRows: fullRows[1][456:1679],
		},
		{
			name:         "only startKey",
			tableName:    tableName(0),
			startKey:     int64(56),
			endKey:       nil,
			expectedRows: fullRows[0][56:],
		},
		{
			name:         "only endKey",
			tableName:    tableName(2),
			startKey:     nil,
			endKey:       int64(2222),
			expectedRows: fullRows[2][:2222],
		},
		{
			name:         "empty range",
			tableName:    tableName(0),
			startKey:     int64(666),
			endKey:       int64(666),
			expectedRows: nil,
		},
		{
			name:         "single row",
			tableName:    tableName(2),
			startKey:     int64(2898),
			endKey:       int64(2899),
			expectedRows: fullRows[2][2898:2899],
		},
		{
			name:         "non existant key between keys",
			tableName:    tableName(1),
			startKey:     "name_1_0166a",
			endKey:       "name_1_0177a",
			expectedRows: fullRows[1][1670:1780],
		},
		{
			name:         "scan empty table",
			tableName:    tableName(3),
			startKey:     int64(3),
			endKey:       int64(1000),
			expectedRows: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scanner, err := db.Scan(tt.tableName, tt.startKey, tt.endKey)
			if err != nil {
				t.Fatalf("failed to scan table %s from %v to %v: %v", tt.tableName, tt.startKey, tt.endKey, err)
			}

			var scannedRows []tuple.Tuple
			for {
				row, err := scanner.Next()
				if err != nil {
					t.Fatalf("error while scanning: %v", err)
				}
				if row == nil {
					break
				}
				scannedRows = append(scannedRows, row)
			}

			if !reflect.DeepEqual(scannedRows, tt.expectedRows) {
				t.Errorf("expected rows not scanned correctly")
			}
		})
	}
}
