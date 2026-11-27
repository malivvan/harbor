package vector

import (
	"testing"

	"github.com/malivvan/tempest/internal/bucket"
	"github.com/stretchr/testify/require"
)

// TestConfigValidateAndNormalize checks the public validation used by Open:
// encryption needs a valid key, and Normalize fills the quantizer default and
// drops empty Root elements.
func TestConfigValidateAndNormalize(t *testing.T) {
	key := make([]byte, KeySize)
	for i := range key {
		key[i] = byte(i + 1)
	}

	require.NoError(t, Config{}.Validate())
	require.NoError(t, Config{Secret: key, EncryptPoints: true}.Validate())
	require.NoError(t, Config{Secret: key, EncryptPointIDs: true, EncryptCollectionNames: true}.Validate())

	require.ErrorIs(t, Config{EncryptPoints: true}.Validate(), ErrInvalidKey)
	require.ErrorIs(t, Config{Secret: []byte("short"), EncryptPointIDs: true}.Validate(), ErrInvalidKey)

	got, err := Config{UseQuant: true, Root: [][]byte{[]byte(bucket.VECTOR), nil}}.Normalize()
	require.NoError(t, err)
	require.NotNil(t, got.Quantizer)
	require.Equal(t, [][]byte{[]byte(bucket.VECTOR)}, got.Root)

	_, err = Config{EncryptPoints: true}.Normalize()
	require.ErrorIs(t, err, ErrInvalidKey)
}

// TestCollectionOptionsValidate checks the option-level validation exposed by
// CollectionOptions.Validate.
func TestCollectionOptionsValidate(t *testing.T) {
	require.ErrorIs(t, CollectionOptions{}.Validate(), ErrInvalidConfig)

	valid := CollectionOptions{Name: "docs", VectorLen: 3, Metric: Cosine, UseHNSW: true}
	require.NoError(t, valid.Validate())

	require.ErrorIs(t, CollectionOptions{Name: "docs", VectorLen: 0, Metric: Cosine}.Validate(), ErrInvalidConfig)
	require.ErrorIs(t, CollectionOptions{VectorLen: 3, Metric: Cosine}.Validate(), ErrInvalidConfig)
	require.ErrorIs(t, CollectionOptions{Name: "docs", VectorLen: 3, Metric: "unknown"}.Validate(), ErrInvalidConfig)
}

func TestHNSWParamsNormalize(t *testing.T) {
	got := HNSWParams{M: -1, EfConstruction: -1, EfSearch: -1, K: -1}.Normalize()
	require.Equal(t, DefaultHNSWParams(), got)

	custom := HNSWParams{M: 32, EfConstruction: 128, EfSearch: 64, K: 5}
	require.Equal(t, custom, custom.Normalize())
}
