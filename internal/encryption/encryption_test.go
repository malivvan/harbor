package encryption

import (
	"bytes"
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

func TestCipherSuiteString(t *testing.T) {
	tests := map[CipherSuite]string{
		CipherAuto:              "auto",
		CipherAES256GCM:         "aes-256-gcm",
		CipherXChaCha20Poly1305: "xchacha20-poly1305",
		CipherSuite(99):         "unknown",
	}
	for suite, want := range tests {
		require.Equal(t, want, suite.String())
	}
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

func TestXChaCha20Poly1305Engine(t *testing.T) {
	engine, err := NewXChaCha20Poly1305Engine(testKey(2))
	require.NoError(t, err)

	require.Equal(t, 24, engine.NonceSize())
	require.Equal(t, 16, engine.Overhead())

	nonce := make([]byte, engine.NonceSize())
	ciphertext, err := engine.Encrypt(nonce, []byte("hello"))
	require.NoError(t, err)

	plaintext, err := engine.Decrypt(nonce, ciphertext)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), plaintext)
}

func TestEngineInvalidKeySize(t *testing.T) {
	for _, size := range []int{0, 1, 16, 24, KeySize - 1, KeySize + 1} {
		_, err := NewAESGCMEngine(make([]byte, size))
		require.Error(t, err, "AES key size %d", size)

		_, err = NewXChaCha20Poly1305Engine(make([]byte, size))
		require.Error(t, err, "XChaCha key size %d", size)
	}
}

func TestEngineInvalidNonceSize(t *testing.T) {
	for _, engine := range []CipherEngine{mustAESEngine(t), mustXChaChaEngine(t)} {
		short := make([]byte, engine.NonceSize()-1)
		_, err := engine.Encrypt(short, []byte("data"))
		require.Error(t, err)
		_, err = engine.Decrypt(short, []byte("data"))
		require.Error(t, err)
	}
}

func TestEngineAuthFailure(t *testing.T) {
	for _, engine := range []CipherEngine{mustAESEngine(t), mustXChaChaEngine(t)} {
		nonce := make([]byte, engine.NonceSize())
		_, err := engine.Decrypt(nonce, []byte("not a valid ciphertext"))
		require.ErrorIs(t, err, ErrAuthFailed)
	}
}

func TestEngineTamperDetection(t *testing.T) {
	engine := mustXChaChaEngine(t)
	nonce := make([]byte, engine.NonceSize())

	ciphertext, err := engine.Encrypt(nonce, []byte("payload"))
	require.NoError(t, err)

	ciphertext[len(ciphertext)-1] ^= 0xFF
	_, err = engine.Decrypt(nonce, ciphertext)
	require.True(t, errors.Is(err, ErrAuthFailed))
}

func TestResolve(t *testing.T) {
	tests := []struct {
		in   CipherSuite
		want CipherSuite
	}{
		{CipherAuto, CipherAES256GCM},
		{CipherAES256GCM, CipherAES256GCM},
		{CipherXChaCha20Poly1305, CipherXChaCha20Poly1305},
	}
	for _, tt := range tests {
		got, err := Resolve(tt.in)
		require.NoError(t, err, "suite %s", tt.in)
		require.Equal(t, tt.want, got)
	}

	_, err := Resolve(CipherSuite(99))
	require.ErrorIs(t, err, ErrUnsupportedCipher)
}

func TestNewCipherEngine(t *testing.T) {
	key := testKey(3)

	for _, suite := range []CipherSuite{CipherAuto, CipherAES256GCM, CipherXChaCha20Poly1305} {
		engine, err := NewCipherEngine(suite, key)
		require.NoError(t, err, "suite %s", suite)
		require.NotNil(t, engine)
	}

	_, err := NewCipherEngine(CipherSuite(99), key)
	require.ErrorIs(t, err, ErrUnsupportedCipher)
}

func TestGenerateNonce(t *testing.T) {
	tests := []struct {
		suite CipherSuite
		size  int
	}{
		{CipherAuto, 12},
		{CipherAES256GCM, 12},
		{CipherXChaCha20Poly1305, 24},
	}
	for _, tt := range tests {
		nonce, err := GenerateNonce(tt.suite)
		require.NoError(t, err, "suite %s", tt.suite)
		require.Len(t, nonce, tt.size, "suite %s", tt.suite)
	}

	first, err := GenerateNonce(CipherXChaCha20Poly1305)
	require.NoError(t, err)
	second, err := GenerateNonce(CipherXChaCha20Poly1305)
	require.NoError(t, err)
	require.False(t, bytes.Equal(first, second), "nonces must be random")

	_, err = GenerateNonce(CipherSuite(99))
	require.ErrorIs(t, err, ErrUnsupportedCipher)
}

func mustAESEngine(t *testing.T) CipherEngine {
	t.Helper()
	engine, err := NewAESGCMEngine(testKey(4))
	require.NoError(t, err)
	return engine
}

func mustXChaChaEngine(t *testing.T) CipherEngine {
	t.Helper()
	engine, err := NewXChaCha20Poly1305Engine(testKey(5))
	require.NoError(t, err)
	return engine
}
