package tables

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// Entity represents a table storage entity with metadata.
type Entity struct {
	PartitionKey string `json:"PartitionKey"`
	RowKey       string `json:"RowKey"`
	Value        []byte `json:"Value,omitempty"`
	Timestamp    string `json:"Timestamp,omitempty"`
}

// entityRequestBody builds the JSON request body for entity write operations.
func entityRequestBody(e Entity) ([]byte, error) {
	reqBody := map[string]interface{}{
		"PartitionKey": e.PartitionKey,
		"RowKey":       e.RowKey,
	}
	if len(e.Value) > 0 {
		reqBody["Value"] = base64.StdEncoding.EncodeToString(e.Value)
	}
	data, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal entity: %w", err)
	}
	return data, nil
}

// entityFromBytes unmarshals entity JSON and builds the request body in one pass.
func entityFromBytes(entData []byte) (Entity, []byte, error) {
	var e Entity
	if err := json.Unmarshal(entData, &e); err != nil {
		return Entity{}, nil, fmt.Errorf("unmarshal entity: %w", err)
	}
	body, err := entityRequestBody(e)
	if err != nil {
		return Entity{}, nil, err
	}
	return e, body, nil
}
