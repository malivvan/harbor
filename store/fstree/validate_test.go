package fstree

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConfigValidateAndNormalize checks the public validation used by Open:
// chunk sizes must be non-negative, encryption needs a valid key, and Normalize
// returns the configuration unchanged.
func TestConfigValidateAndNormalize(t *testing.T) {
	key := testEncKey(3)

	require.NoError(t, Config{}.Validate())
	require.NoError(t, Config{Secret: key, ChunkSize: 4096}.Validate())
	require.NoError(t, Config{Secret: key, EncryptMetadata: true}.Validate())

	require.ErrorIs(t, Config{ChunkSize: -1}.Validate(), ErrInvalidConfig)
	require.ErrorIs(t, Config{ChunkSize: 4096}.Validate(), ErrInvalidKey)
	require.ErrorIs(t, Config{EncryptContent: true}.Validate(), ErrInvalidKey)
	require.ErrorIs(t, Config{Secret: []byte("short"), EncryptContent: true}.Validate(), ErrInvalidKey)

	got, err := Config{Secret: key, ChunkSize: 16}.Normalize()
	require.NoError(t, err)
	require.Equal(t, 16, got.ChunkSize)
	require.Equal(t, key, got.Secret)

	_, err = Config{ChunkSize: -1}.Normalize()
	require.ErrorIs(t, err, ErrInvalidConfig)
}
