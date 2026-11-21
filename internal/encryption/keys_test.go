package encryption

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveKey(t *testing.T) {
	key := testKey(5)

	got, err := ResolveKey(key)
	require.NoError(t, err)
	require.Equal(t, key, got)

	// The result must not alias the caller's buffer.
	got[0] ^= 0xff
	require.Equal(t, byte(5), key[0])

	_, err = ResolveKey(key[:8])
	require.ErrorIs(t, err, ErrInvalidKey)
}
