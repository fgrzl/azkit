package credentials

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validBase64AccountKey is a valid base64-encoded key for SharedKey tests.
const validBase64AccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

func TestShouldCreateCredentialWhenGivenValidAccountNameAndKey(t *testing.T) {
	// Arrange
	accountName := "myaccount"
	accountKey := validBase64AccountKey

	// Act
	cred, err := NewSharedKeyCredential(accountName, accountKey)

	// Assert
	require.NoError(t, err)
	assert.NotNil(t, cred)
	assert.Equal(t, accountName, cred.AccountName)
	assert.Equal(t, accountKey, cred.AccountKey)
}

func TestShouldReturnCredentialErrorWhenAccountNameIsEmpty(t *testing.T) {
	// Arrange & Act
	cred, err := NewSharedKeyCredential("", validBase64AccountKey)

	// Assert
	require.Error(t, err)
	assert.Nil(t, cred)
	assert.Contains(t, err.Error(), "account name is required")
}

func TestShouldReturnCredentialErrorWhenAccountKeyIsEmpty(t *testing.T) {
	// Arrange & Act
	cred, err := NewSharedKeyCredential("myaccount", "")

	// Assert
	require.Error(t, err)
	assert.Nil(t, cred)
	assert.Contains(t, err.Error(), "account key is required")
}

func TestShouldReturnCredentialErrorWhenAccountKeyIsNotValidBase64(t *testing.T) {
	// Arrange & Act
	cred, err := NewSharedKeyCredential("myaccount", "not-valid-base64!!!")

	// Assert
	require.Error(t, err)
	assert.Nil(t, cred)
	assert.Contains(t, err.Error(), "valid base64")
}

func TestShouldReturnNonNilCredentialWhenCreatingManagedIdentity(t *testing.T) {
	// Arrange & Act
	cred := NewManagedIdentityCredential("")

	// Assert
	require.NotNil(t, cred)
}

func TestShouldAcceptClientIDWhenCreatingManagedIdentityCredential(t *testing.T) {
	// Arrange
	clientID := "some-client-id"

	// Act
	cred := NewManagedIdentityCredential(clientID)

	// Assert
	require.NotNil(t, cred)
}
