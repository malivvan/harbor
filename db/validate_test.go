package db

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func testConfigKey() []byte {
	return bytes.Repeat([]byte{7}, KeySize)
}

// TestConfigValidateAndNormalize checks the public validation used by Open:
// plaintext configurations need nothing, encryption needs a valid key, and
// Normalize fills the default codec without changing anything else.
func TestConfigValidateAndNormalize(t *testing.T) {
	key := testConfigKey()

	require.NoError(t, Config{}.Validate())
	require.NoError(t, Config{Codec: JSONCodec}.Validate())
	require.NoError(t, Config{Secret: key, EncryptValues: true}.Validate())
	require.NoError(t, Config{Secret: key, EncryptKeys: true, EncryptMetadata: true}.Validate())

	require.ErrorIs(t, Config{EncryptValues: true}.Validate(), ErrInvalidKey)
	require.ErrorIs(t, Config{Secret: []byte("short"), EncryptKeys: true}.Validate(), ErrInvalidKey)

	// Normalize applies the codec default and keeps a supplied codec.
	got, err := Config{}.Normalize()
	require.NoError(t, err)
	require.Equal(t, JSONCodec, got.Codec)

	got, err = Config{Codec: JSONCodec, Secret: key, EncryptValues: true}.Normalize()
	require.NoError(t, err)
	require.Equal(t, JSONCodec, got.Codec)
	require.Equal(t, key, got.Secret)

	_, err = Config{Secret: []byte("short"), EncryptValues: true}.Normalize()
	require.ErrorIs(t, err, ErrInvalidKey)
}
