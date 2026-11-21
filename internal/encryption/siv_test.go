package encryption

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeriveSIV(t *testing.T) {
	master := testKey(20)

	engine, err := DeriveSIV(master, "storm/test/keys/id/v1")
	require.NoError(t, err)

	// Deterministic: the same master key and info always derive the same engine.
	again, err := DeriveSIV(master, "storm/test/keys/id/v1")
	require.NoError(t, err)

	first, err := engine.Encrypt([]byte("value"))
	require.NoError(t, err)
	second, err := again.Encrypt([]byte("value"))
	require.NoError(t, err)
	require.Equal(t, first, second)

	// Different info yields an independent key.
	other, err := DeriveSIV(master, "storm/test/keys/name/v1")
	require.NoError(t, err)
	third, err := other.Encrypt([]byte("value"))
	require.NoError(t, err)
	require.NotEqual(t, first, third, "domains must be separated")

	_, err = DeriveSIV(master[:16], "storm/test/keys/id/v1")
	require.ErrorIs(t, err, ErrInvalidKey)
}

func TestSIVNameCipher(t *testing.T) {
	cipher, err := NewSIVNameCipher(testKey(21), "storm/test/keys/id/v1")
	require.NoError(t, err)

	first, err := cipher.SealName([]byte("point-1"))
	require.NoError(t, err)
	second, err := cipher.SealName([]byte("point-1"))
	require.NoError(t, err)
	other, err := cipher.SealName([]byte("point-2"))
	require.NoError(t, err)

	require.Equal(t, first, second, "the same name must map to the same key")
	require.NotEqual(t, first, other)
	require.NotContains(t, string(first), "point-1")

	// The encoding must never contain the separator composite index keys use.
	require.False(t, bytes.ContainsRune(first, '_'), "encoded name must not contain an underscore")
	require.False(t, bytes.ContainsAny(first, "="), "the encoding must be unpadded")

	name, err := cipher.OpenName(first)
	require.NoError(t, err)
	require.Equal(t, []byte("point-1"), name)

	tampered := append([]byte(nil), first...)
	tampered[0] ^= 0x01
	_, err = cipher.OpenName(tampered)
	require.ErrorIs(t, err, ErrAuthFailed)

	_, err = cipher.OpenName([]byte("not base64!!"))
	require.ErrorIs(t, err, ErrAuthFailed, "a malformed name is an authentication failure")

	_, err = NewSIVNameCipher(testKey(21)[:16], "storm/test/keys/id/v1")
	require.ErrorIs(t, err, ErrInvalidKey)
}

func TestNilSIVNameCipherIsPlaintext(t *testing.T) {
	var cipher *SIVNameCipher

	sealed, err := cipher.SealName([]byte("collection"))
	require.NoError(t, err)
	require.Equal(t, []byte("collection"), sealed)

	name, err := cipher.OpenName([]byte("collection"))
	require.NoError(t, err)
	require.Equal(t, []byte("collection"), name)
}

func TestSIVNameCipherDomainSeparation(t *testing.T) {
	master := testKey(22)

	idCipher, err := NewSIVNameCipher(master, "storm/test/keys/id/v1")
	require.NoError(t, err)
	nameCipher, err := NewSIVNameCipher(master, "storm/test/keys/name/v1")
	require.NoError(t, err)

	asID, err := idCipher.SealName([]byte("shared"))
	require.NoError(t, err)
	asName, err := nameCipher.SealName([]byte("shared"))
	require.NoError(t, err)
	require.NotEqual(t, asID, asName)

	// A key from one domain must not authenticate in the other.
	_, err = nameCipher.OpenName(asID)
	require.ErrorIs(t, err, ErrAuthFailed)
}
