package validate

import (
	"encoding/base64"
	"fmt"
)

// AccountKeyBase64 returns an error if key is not valid base64.
func AccountKeyBase64(key string) error {
	if _, err := base64.StdEncoding.DecodeString(key); err != nil {
		return fmt.Errorf("account key must be valid base64: %w", err)
	}
	return nil
}
