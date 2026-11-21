package encryption

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewKeychain(t *testing.T) {
	key := testKey(10)

	kc, err := NewKeychain(CipherAuto, [][]byte{key})
	require.NoError(t, err)
	require.Equal(t, CipherAES256GCM, kc.Cipher(), "CipherAuto resolves to AES-256-GCM")
	require.NotNil(t, kc.PrimaryEngine())
	require.Equal(t, [][]byte{key}, kc.Keys())

	// Keys must be copies.
	keys := kc.Keys()
	keys[0][0] ^= 0xff
	require.Equal(t, byte(10), kc.Keys()[0][0])

	_, err = NewKeychain(CipherAuto, nil)
	require.ErrorIs(t, err, ErrInvalidConfig)

	_, err = NewKeychain(CipherAuto, [][]byte{key[:8]})
	require.ErrorIs(t, err, ErrInvalidKey)

	_, err = NewKeychain(CipherSuite(99), [][]byte{key})
	require.ErrorIs(t, err, ErrUnsupportedCipher)

	xchacha, err := NewKeychain(CipherXChaCha20Poly1305, [][]byte{testKey(11)})
	require.NoError(t, err)
	require.Equal(t, CipherXChaCha20Poly1305, xchacha.Cipher())
}

func TestNilKeychainIsPlaintext(t *testing.T) {
	var kc *Keychain

	require.Equal(t, CipherAuto, kc.Cipher())
	require.Nil(t, kc.Keys())
	require.Nil(t, kc.PrimaryEngine())

	sealed, err := kc.Seal([]byte("plain"))
	require.NoError(t, err)
	require.Equal(t, []byte("plain"), sealed)

	plain, err := kc.Open([]byte("plain"))
	require.NoError(t, err)
	require.Equal(t, []byte("plain"), plain)
}

func TestKeychainSealOpen(t *testing.T) {
	kc, err := NewKeychain(CipherAES256GCM, [][]byte{testKey(12)})
	require.NoError(t, err)

	first, err := kc.Seal([]byte("secret"))
	require.NoError(t, err)
	second, err := kc.Seal([]byte("secret"))
	require.NoError(t, err)
	require.NotEqual(t, first, second, "every seal uses a fresh nonce")
	require.NotContains(t, string(first), "secret")

	plain, err := kc.Open(first)
	require.NoError(t, err)
	require.Equal(t, []byte("secret"), plain)

	tampered := append([]byte(nil), first...)
	tampered[len(tampered)-1] ^= 0xff
	_, err = kc.Open(tampered)
	require.ErrorIs(t, err, ErrAuthFailed)

	_, err = kc.Open(first[:4])
	require.ErrorIs(t, err, ErrAuthFailed, "truncated ciphertext must not panic or pass")
}

func TestKeychainEmptyPassthrough(t *testing.T) {
	kc, err := NewKeychain(CipherAES256GCM, [][]byte{testKey(13)})
	require.NoError(t, err)

	for _, empty := range [][]byte{nil, {}} {
		sealed, err := kc.Seal(empty)
		require.NoError(t, err)
		require.Empty(t, sealed, "an empty payload is never sealed")

		plain, err := kc.Open(empty)
		require.NoError(t, err)
		require.Empty(t, plain, "an empty payload opens as itself")
	}
}

func TestKeychainFallbackKeys(t *testing.T) {
	oldKey := testKey(14)
	newKey := testKey(15)

	oldChain, err := NewKeychain(CipherAuto, [][]byte{oldKey})
	require.NoError(t, err)
	oldSealed, err := oldChain.Seal([]byte("from the old key"))
	require.NoError(t, err)

	both, err := NewKeychain(CipherAuto, [][]byte{newKey, oldKey})
	require.NoError(t, err)

	plain, err := both.Open(oldSealed)
	require.NoError(t, err)
	require.Equal(t, []byte("from the old key"), plain)

	// New writes use the primary key, which the old key cannot read.
	fresh, err := both.Seal([]byte("from the new key"))
	require.NoError(t, err)
	_, err = oldChain.Open(fresh)
	require.ErrorIs(t, err, ErrAuthFailed)
}

func TestKeychainWithKey(t *testing.T) {
	oldKey := testKey(16)
	newKey := testKey(17)

	kc, err := NewKeychain(CipherAuto, [][]byte{oldKey})
	require.NoError(t, err)

	next, err := kc.WithKey(newKey)
	require.NoError(t, err)
	require.Equal(t, [][]byte{newKey}, next.Keys(), "WithKey drops fallbacks")
	require.Equal(t, kc.Cipher(), next.Cipher())

	// Data sealed with the old key is no longer readable.
	sealed, err := kc.Seal([]byte("old payload"))
	require.NoError(t, err)
	_, err = next.Open(sealed)
	require.ErrorIs(t, err, ErrAuthFailed)

	var plaintext *Keychain
	_, err = plaintext.WithKey(newKey)
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = plaintext.WithKeyKeepingFallbacks(newKey)
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestKeychainWithKeyKeepingFallbacks(t *testing.T) {
	oldKey := testKey(18)
	newKey := testKey(19)

	kc, err := NewKeychain(CipherAuto, [][]byte{oldKey})
	require.NoError(t, err)

	next, err := kc.WithKeyKeepingFallbacks(newKey)
	require.NoError(t, err)
	require.Equal(t, [][]byte{newKey, oldKey}, next.Keys(), "the previous key stays as a fallback")

	oldSealed, err := kc.Seal([]byte("old payload"))
	require.NoError(t, err)
	plain, err := next.Open(oldSealed)
	require.NoError(t, err)
	require.Equal(t, []byte("old payload"), plain)

	// New payloads use the primary key, and the rewritten entity is readable
	// with the new key alone.
	newSealed, err := next.Seal([]byte("new payload"))
	require.NoError(t, err)
	onlyNew, err := NewKeychain(CipherAuto, [][]byte{newKey})
	require.NoError(t, err)
	plain, err = onlyNew.Open(newSealed)
	require.NoError(t, err)
	require.Equal(t, []byte("new payload"), plain)

	require.False(t, bytes.Equal(oldSealed, newSealed))
}
