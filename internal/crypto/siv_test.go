package crypto

import (
	"bytes"
	"encoding/hex"
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

// testKeyBytes returns a deterministic key of an arbitrary size.
func testKeyBytes(seed, size int) []byte {
	key := make([]byte, size)
	for i := range key {
		key[i] = byte(seed + i)
	}
	return key
}

// hexBytes decodes a whitespace-free hex string for the RFC vectors below.
func hexBytes(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

// TestSIVRFC5297Vectors checks the S2V/CMAC/CTR implementation against the
// published AES-SIV known-answer vectors (RFC 5297, appendix A).
func TestSIVRFC5297Vectors(t *testing.T) {
	t.Run("A.1 deterministic", func(t *testing.T) {
		engine, err := NewSIVEngine(hexBytes(t, ""+
			"fffefdfcfbfaf9f8f7f6f5f4f3f2f1f0"+
			"f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff"))
		require.NoError(t, err)

		ad := hexBytes(t, "101112131415161718191a1b1c1d1e1f2021222324252627")
		plaintext := hexBytes(t, "112233445566778899aabbccddee")
		want := hexBytes(t, ""+
			"85632d07c6e8f37f950acd320a2ecc93"+
			"40c02b9690c4dc04daef7f6afe5c")

		got, err := engine.Encrypt(plaintext, ad)
		require.NoError(t, err)
		require.Equal(t, want, got)

		decrypted, err := engine.Decrypt(want, ad)
		require.NoError(t, err)
		require.Equal(t, plaintext, decrypted)
	})

	t.Run("A.2 nonce-based", func(t *testing.T) {
		engine, err := NewSIVEngine(hexBytes(t, ""+
			"7f7e7d7c7b7a79787776757473727170"+
			"404142434445464748494a4b4c4d4e4f"))
		require.NoError(t, err)

		ad1 := hexBytes(t, ""+
			"00112233445566778899aabbccddeeff"+
			"deaddadadeaddadaffeeddccbbaa9988"+
			"7766554433221100")
		ad2 := hexBytes(t, "102030405060708090a0")
		nonce := hexBytes(t, "09f911029d74e35bd84156c5635688c0")
		plaintext := hexBytes(t, ""+
			"7468697320697320736f6d6520706c61"+
			"696e7465787420746f20656e63727970"+
			"74207573696e67205349562d414553")
		want := hexBytes(t, ""+
			"7bdb6e3b432667eb06f4d14bff2fbd0f"+
			"cb900f2fddbe404326601965c889bf17"+
			"dba77ceb094fa663b7a3f748ba8af829"+
			"ea64ad544a272e9c485b62a3fd5c0d")

		got, err := engine.Encrypt(plaintext, ad1, ad2, nonce)
		require.NoError(t, err)
		require.Equal(t, want, got)

		decrypted, err := engine.Decrypt(want, ad1, ad2, nonce)
		require.NoError(t, err)
		require.Equal(t, plaintext, decrypted)
	})
}

func TestSIVEmptyInputs(t *testing.T) {
	engine, err := NewSIVEngine(testKeyBytes(30, 64))
	require.NoError(t, err)

	// Empty plaintext with empty associated data still authenticates: the
	// ciphertext is exactly the SIV.
	sealed, err := engine.Encrypt(nil)
	require.NoError(t, err)
	require.Len(t, sealed, engine.Overhead())

	opened, err := engine.Decrypt(sealed)
	require.NoError(t, err)
	require.Empty(t, opened)

	// An explicit empty associated-data element is still an element and must
	// round-trip.
	withEmptyAD, err := engine.Encrypt(nil, []byte{})
	require.NoError(t, err)
	opened, err = engine.Decrypt(withEmptyAD, []byte{})
	require.NoError(t, err)
	require.Empty(t, opened)

	// A tampered SIV is rejected.
	sealed[0] ^= 0x01
	_, err = engine.Decrypt(sealed)
	require.ErrorIs(t, err, ErrAuthFailed)
}

func TestSIVRejectsShortCiphertext(t *testing.T) {
	engine, err := NewSIVEngine(testKeyBytes(31, 64))
	require.NoError(t, err)

	for _, ct := range [][]byte{nil, {}, make([]byte, 15)} {
		_, err := engine.Decrypt(ct)
		require.ErrorIs(t, err, ErrAuthFailed, "short ciphertext must be an auth failure")
	}
}

func TestSIVKeySizesAndIsolation(t *testing.T) {
	for _, size := range []int{32, 48, 64} {
		key := testKeyBytes(32+size, size)
		engine, err := NewSIVEngine(key)
		require.NoError(t, err, "size %d", size)
		wantKey := append([]byte(nil), key...)

		// The engine must copy the key: mutating the caller's buffer must not
		// change later results.
		for i := range key {
			key[i] = 0
		}
		got, err := engine.Encrypt([]byte("value"))
		require.NoError(t, err)
		fresh, err := NewSIVEngine(wantKey)
		require.NoError(t, err)
		expected, err := fresh.Encrypt([]byte("value"))
		require.NoError(t, err)
		require.Equal(t, expected, got, "engine aliased the caller key")
	}

	for _, size := range []int{0, 16, 31, 33, 65} {
		_, err := NewSIVEngine(make([]byte, size))
		require.ErrorIs(t, err, ErrInvalidKey, "size %d", size)
	}
}

func TestSIVPadNeverPanics(t *testing.T) {
	require.Equal(t, []byte{
		0x80, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
	}, pad(nil))
	require.NotPanics(t, func() { pad(make([]byte, 16)) })
	require.NotPanics(t, func() { pad(make([]byte, 32)) })
}

func TestSIVNameCipherRejectsShortNames(t *testing.T) {
	cipher, err := NewSIVNameCipher(testKey(33), "storm/test/keys/id/v1")
	require.NoError(t, err)

	for _, encoded := range [][]byte{nil, []byte(""), []byte("AAAA"), []byte("AAAAAA")} {
		_, err := cipher.OpenName(encoded)
		require.ErrorIs(t, err, ErrAuthFailed, "encoded name %q", encoded)
	}
}
