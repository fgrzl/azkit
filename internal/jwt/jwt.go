package jwt

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

// StorageAudience is the expected audience for Azure Storage tokens.
const StorageAudience = "https://storage.azure.com"

// DecodeClaims extracts the payload claims from a JWT without signature validation.
func DecodeClaims(token string) (map[string]any, error) {
	parts := strings.SplitN(token, ".", 3)
	if len(parts) < 2 {
		return nil, fmt.Errorf("token is not a valid JWT")
	}

	payload := parts[1]
	switch len(payload) % 4 {
	case 2:
		payload += "=="
	case 3:
		payload += "="
	}

	decoded, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		return nil, fmt.Errorf("decode JWT payload: %w", err)
	}

	var claims map[string]any
	if err := json.Unmarshal(decoded, &claims); err != nil {
		return nil, fmt.Errorf("parse JWT claims: %w", err)
	}
	return claims, nil
}

// ClaimString returns a string claim value, or empty string if missing or wrong type.
func ClaimString(claims map[string]any, key string) string {
	v, _ := claims[key].(string)
	return v
}

// CheckStorageAudience returns an error if aud is set and does not match storage.azure.com.
func CheckStorageAudience(claims map[string]any) error {
	aud := ClaimString(claims, "aud")
	if aud == "" {
		return nil
	}
	if aud == StorageAudience || aud == StorageAudience+"/" {
		return nil
	}
	return fmt.Errorf("token audience %q does not match expected %q", aud, StorageAudience)
}
