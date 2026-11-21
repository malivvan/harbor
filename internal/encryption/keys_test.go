package encryption

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRawKeyProvider(t *testing.T) {
	key := testKey(1)

	provider, err := NewRawKeyProvider(key)
	require.NoError(t, err)

	got, err := provider.Key()
	require.NoError(t, err)
	require.Equal(t, key, got)

	// The provider must not expose its own buffer.
	got[0] ^= 0xff
	again, err := provider.Key()
	require.NoError(t, err)
	require.Equal(t, key, again)

	_, err = NewRawKeyProvider(key[:16])
	require.ErrorIs(t, err, ErrInvalidKey)
}

func TestEnvKeyProvider(t *testing.T) {
	const envVar = "STORM_TEST_ENCRYPTION_KEY"
	key := testKey(2)

	t.Setenv(envVar, "")
	provider := NewEnvKeyProvider(envVar)
	_, err := provider.Key()
	require.ErrorIs(t, err, ErrInvalidKey, "an unset variable is an invalid key")

	t.Setenv(envVar, string(key[:8]))
	_, err = provider.Key()
	require.ErrorIs(t, err, ErrInvalidKey, "a short key is an invalid key")

	t.Setenv(envVar, string(key))
	got, err := provider.Key()
	require.NoError(t, err)
	require.Equal(t, key, got)
}

func TestMultiKeyProvider(t *testing.T) {
	primary := testKey(3)
	fallback := testKey(4)

	primaryProvider, err := NewRawKeyProvider(primary)
	require.NoError(t, err)
	fallbackProvider, err := NewRawKeyProvider(fallback)
	require.NoError(t, err)

	multi, err := NewMultiKeyProvider(primaryProvider, fallbackProvider)
	require.NoError(t, err)

	got, err := multi.Key()
	require.NoError(t, err)
	require.Equal(t, primary, got, "Key returns the primary key")

	keys, err := multi.TryKeys()
	require.NoError(t, err)
	require.Equal(t, [][]byte{primary, fallback}, keys)

	require.Equal(t, []KeyProvider{primaryProvider, fallbackProvider}, multi.Providers())

	_, err = NewMultiKeyProvider()
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestResolveKeys(t *testing.T) {
	key := testKey(5)

	t.Run("raw key", func(t *testing.T) {
		keys, err := ResolveKeys(key, nil)
		require.NoError(t, err)
		require.Equal(t, [][]byte{key}, keys)

		// The result must not alias the caller's buffer.
		keys[0][0] ^= 0xff
		require.Equal(t, byte(5), key[0])
	})

	t.Run("invalid raw key", func(t *testing.T) {
		_, err := ResolveKeys(key[:8], nil)
		require.ErrorIs(t, err, ErrInvalidKey)
	})

	t.Run("both key and provider", func(t *testing.T) {
		provider, err := NewRawKeyProvider(key)
		require.NoError(t, err)

		_, err = ResolveKeys(key, provider)
		require.ErrorIs(t, err, ErrInvalidConfig)
	})

	t.Run("provider", func(t *testing.T) {
		provider, err := NewRawKeyProvider(key)
		require.NoError(t, err)

		keys, err := ResolveKeys(nil, provider)
		require.NoError(t, err)
		require.Equal(t, [][]byte{key}, keys)
	})

	t.Run("multi provider expands in order", func(t *testing.T) {
		primary, err := NewRawKeyProvider(testKey(6))
		require.NoError(t, err)
		fallback, err := NewRawKeyProvider(testKey(7))
		require.NoError(t, err)
		multi, err := NewMultiKeyProvider(primary, fallback)
		require.NoError(t, err)

		keys, err := ResolveKeys(nil, multi)
		require.NoError(t, err)
		require.Len(t, keys, 2)
		require.Equal(t, testKey(6), keys[0])
		require.Equal(t, testKey(7), keys[1])
	})

	t.Run("provider error propagates", func(t *testing.T) {
		_, err := ResolveKeys(nil, failingProvider{})
		require.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("provider returns short key", func(t *testing.T) {
		_, err := ResolveKeys(nil, shortProvider{})
		require.ErrorIs(t, err, ErrInvalidKey)
	})

	t.Run("empty multi provider", func(t *testing.T) {
		// A MultiKeyProvider cannot be constructed empty, but ResolveKeys must
		// still refuse a provider list that yields no keys.
		multi := &MultiKeyProvider{}
		_, err := ResolveKeys(nil, multi)
		require.ErrorIs(t, err, ErrInvalidConfig)
	})
}

func TestRequireSingleKey(t *testing.T) {
	require.NoError(t, RequireSingleKey(nil))
	require.NoError(t, RequireSingleKey([][]byte{testKey(8)}))

	err := RequireSingleKey([][]byte{testKey(8), testKey(9)})
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "single master key")
}

// failingProvider always fails, simulating an unavailable key source.
type failingProvider struct{}

func (failingProvider) Key() ([]byte, error) { return nil, os.ErrNotExist }

// shortProvider returns a key of the wrong length.
type shortProvider struct{}

func (shortProvider) Key() ([]byte, error) { return []byte("short"), nil }
