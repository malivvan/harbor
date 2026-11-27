package series

import (
	"testing"

	"github.com/malivvan/tempest/internal/bucket"
	"github.com/stretchr/testify/require"
)

// TestConfigValidateAndNormalize checks the public validation used by Open: the
// block duration must be non-negative, encryption requires a non-empty Root and
// a valid key, and Normalize returns the configuration unchanged.
func TestConfigValidateAndNormalize(t *testing.T) {
	key := testKey(3)
	root := [][]byte{[]byte(bucket.SERIES)}

	require.NoError(t, Config{}.Validate())
	require.NoError(t, Config{Root: root, Secret: key, EncryptValues: true}.Validate())
	require.NoError(t, Config{Root: root, Secret: key, BlockDuration: 1000}.Validate())
	require.NoError(t, Config{Root: root, Secret: key, EncryptSeriesNames: true}.Validate())

	require.ErrorIs(t, Config{BlockDuration: -1}.Validate(), ErrInvalidConfig)
	require.ErrorIs(t, Config{Secret: key, EncryptValues: true}.Validate(), ErrInvalidConfig)
	require.ErrorIs(t, Config{Root: [][]byte{nil}, Secret: key, EncryptValues: true}.Validate(), ErrInvalidConfig)
	require.ErrorIs(t, Config{Root: root, EncryptValues: true}.Validate(), ErrInvalidKey)
	require.ErrorIs(t, Config{Root: root, Secret: []byte("short"), BlockDuration: 1}.Validate(), ErrInvalidKey)

	got, err := Config{Root: root, Secret: key, EncryptValues: true}.Normalize()
	require.NoError(t, err)
	require.Equal(t, key, got.Secret)
	require.Equal(t, root, got.Root)

	_, err = Config{BlockDuration: -1}.Normalize()
	require.ErrorIs(t, err, ErrInvalidConfig)
}
