package tables

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEntityFromBytesEncodesValue(t *testing.T) {
	raw := []byte(`{"PartitionKey":"p","RowKey":"r","Value":"dmFsdWU="}`)
	_, data, err := entityFromBytes(raw)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"PartitionKey":"p"`)
}

func TestEntityFromBytesReturnsErrorWhenInvalid(t *testing.T) {
	_, _, err := entityFromBytes([]byte("{"))
	require.Error(t, err)
}
