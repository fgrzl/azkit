package jwt

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldDecodeClaimsWhenTokenIsValid(t *testing.T) {
	payload, err := json.Marshal(map[string]string{
		"aud": StorageAudience,
		"iss": "https://sts.windows.net/tenant/",
		"oid": "object-id",
	})
	require.NoError(t, err)

	token := "header." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"

	claims, err := DecodeClaims(token)
	require.NoError(t, err)
	assert.Equal(t, StorageAudience, ClaimString(claims, "aud"))
	assert.NoError(t, CheckStorageAudience(claims))
}

func TestShouldReturnErrorWhenTokenIsNotJWT(t *testing.T) {
	_, err := DecodeClaims("not-a-jwt")
	require.Error(t, err)
}

func TestShouldReturnErrorWhenAudienceMismatches(t *testing.T) {
	claims := map[string]any{"aud": "https://wrong.audience"}
	err := CheckStorageAudience(claims)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "audience")
}
