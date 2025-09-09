// Package catalog
package catalog

type DataType uint8

const (
	TypeInt DataType = iota
	TypeVarChar
	TypeBoolean
	TypeBlob
	TypeFloat
)

type Column struct {
	Name         string   `json:"name"`
	Type         DataType `json:"type"`
	IsPrimaryKey bool     `json:"is_primary_key,omitempty"`
	IsNotNull    bool     `json:"is_not_null,omitempty"`
}

type Schema struct {
	ID              uint32   `json:"id"`
	Name            string   `json:"name"`
	Columns         []Column `json:"columns"`
	PrimaryKeyIndex int      `json:"pk_index"`
}
