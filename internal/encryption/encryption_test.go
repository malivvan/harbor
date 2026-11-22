package encryption

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func testKey(seed byte) []byte {
	key := make([]byte, KeySize)
	for i := range key {
		key[i] = seed + byte(i)
	}
	return key
}

func TestAESGCMEngine(t *testing.T) {
	engine, err := NewAESGCMEngine(testKey(1))
	require.NoError(t, err)

	require.Equal(t, 12, engine.NonceSize())
	require.Equal(t, 16, engine.Overhead())

	nonce := make([]byte, engine.NonceSize())
	ciphertext, err := engine.Encrypt(nonce, []byte("hello"))
	require.NoError(t, err)
	require.Len(t, ciphertext, len("hello")+engine.Overhead())

	plaintext, err := engine.Decrypt(nonce, ciphertext)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), plaintext)
}

func TestEngineInvalidKeySize(t *testing.T) {
	for _, size := range []int{0, 1, 16, 24, KeySize - 1, KeySize + 1} {
		_, err := NewAESGCMEngine(make([]byte, size))
		require.ErrorIs(t, err, ErrInvalidKey, "key size %d", size)
	}
}

func TestEngineInvalidNonceSize(t *testing.T) {
	engine := mustAESEngine(t)

	short := make([]byte, engine.NonceSize()-1)
	_, err := engine.Encrypt(short, []byte("data"))
	require.Error(t, err)
	_, err = engine.Decrypt(short, []byte("data"))
	require.Error(t, err)
}

func TestEngineAuthFailure(t *testing.T) {
	engine := mustAESEngine(t)

	nonce := make([]byte, engine.NonceSize())
	_, err := engine.Decrypt(nonce, []byte("not a valid ciphertext"))
	require.ErrorIs(t, err, ErrAuthFailed)
}

func TestEngineTamperDetection(t *testing.T) {
	engine := mustAESEngine(t)
	nonce := make([]byte, engine.NonceSize())

	ciphertext, err := engine.Encrypt(nonce, []byte("payload"))
	require.NoError(t, err)

	ciphertext[len(ciphertext)-1] ^= 0xFF
	_, err = engine.Decrypt(nonce, ciphertext)
	require.True(t, errors.Is(err, ErrAuthFailed))
}

func mustAESEngine(t *testing.T) *AESGCMEngine {
	t.Helper()
	engine, err := NewAESGCMEngine(testKey(4))
	require.NoError(t, err)
	return engine
}

func TestEngineValidationAndCanaryHelpers(t *testing.T) {
	_, err := NewAESGCMEngine(make([]byte, KeySize-1))
	require.ErrorIs(t, err, ErrInvalidKey)
	_, err = NewAESGCMEngine(make([]byte, KeySize+1))
	require.ErrorIs(t, err, ErrInvalidKey)

	engine, err := NewAESGCMEngine(testKey(71))
	require.NoError(t, err)
	_, err = engine.SealInto(nil, []byte("short"), []byte("plain"))
	require.Error(t, err)

	dst := make([]byte, 0, 64)
	sealed, err := engine.SealInto(dst, make([]byte, engine.NonceSize()), []byte("plain"))
	require.NoError(t, err)
	require.NotEmpty(t, sealed)

	require.Equal(t, "base/0x01/0x0000000000000002", CanaryPlaintext("base", 1, 2))

	siv, err := NewSIVEngine(make([]byte, 32))
	require.NoError(t, err)
	require.Zero(t, siv.NonceSize())
	require.Equal(t, 16, siv.Overhead())
	_, err = NewSIVEngine(make([]byte, 31))
	require.ErrorIs(t, err, ErrInvalidKey)
}
