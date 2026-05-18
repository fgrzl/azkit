package validate

import (
	"testing"

	"github.com/fgrzl/azkit/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestShouldAcceptValidBase64AccountKey(t *testing.T) {
	require.NoError(t, AccountKeyBase64(testutil.ValidBase64AccountKey))
}

func TestShouldRejectInvalidBase64AccountKey(t *testing.T) {
	err := AccountKeyBase64("not-valid!!!")
	require.Error(t, err)
	require.Contains(t, err.Error(), "base64")
}
