package aead

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/storm/db/codec/internal"
	"github.com/malivvan/storm/db/codec/json"
)

func testKey(seed byte) []byte {
	key := make([]byte, KeySize)
	for i := range key {
		key[i] = seed + byte(i)
	}
	return key
}

func TestRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
	}{
		{name: "default", opts: nil},
		{name: "auto", opts: []Option{WithCipher(CipherAuto)}},
		{name: "aes-256-gcm", opts: []Option{WithCipher(CipherAES256GCM)}},
		{name: "xchacha20-poly1305", opts: []Option{WithCipher(CipherXChaCha20Poly1305)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := New(json.Codec, testKey(0x11), tt.opts...)
			require.NoError(t, err)
			internal.RoundtripTester(t, c)
		})
	}
}

func TestName(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
		want string
	}{
		{name: "default", want: "aead-aes-256-gcm-json"},
		{name: "aes", opts: []Option{WithCipher(CipherAES256GCM)}, want: "aead-aes-256-gcm-json"},
		{name: "xchacha", opts: []Option{WithCipher(CipherXChaCha20Poly1305)}, want: "aead-xchacha20-poly1305-json"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := New(json.Codec, testKey(1), tt.opts...)
			require.NoError(t, err)
			require.Equal(t, tt.want, c.Name())
		})
	}
}

func TestCipherSelection(t *testing.T) {
	c, err := New(json.Codec, testKey(1))
	require.NoError(t, err)
	require.Equal(t, CipherAES256GCM, c.Cipher(), "auto must resolve to AES-256-GCM")

	c, err = New(json.Codec, testKey(1), WithCipher(CipherXChaCha20Poly1305))
	require.NoError(t, err)
	require.Equal(t, CipherXChaCha20Poly1305, c.Cipher())
}

func TestWrongKeyFailsAuthentication(t *testing.T) {
	writer, err := New(json.Codec, testKey(1))
	require.NoError(t, err)

	reader, err := New(json.Codec, testKey(2))
	require.NoError(t, err)

	encoded, err := writer.Marshal(&struct{ Name string }{Name: "secret"})
	require.NoError(t, err)

	var out struct{ Name string }
	err = reader.Unmarshal(encoded, &out)
	require.ErrorIs(t, err, ErrAuthFailed)
}

func TestTamperedCiphertextFailsAuthentication(t *testing.T) {
	c, err := New(json.Codec, testKey(3), WithCipher(CipherXChaCha20Poly1305))
	require.NoError(t, err)

	encoded, err := c.Marshal(&struct{ Value int }{Value: 42})
	require.NoError(t, err)

	encoded[len(encoded)-1] ^= 0xFF

	var out struct{ Value int }
	require.ErrorIs(t, c.Unmarshal(encoded, &out), ErrAuthFailed)
}

func TestTruncatedCiphertext(t *testing.T) {
	c, err := New(json.Codec, testKey(4))
	require.NoError(t, err)

	var out struct{ Value int }
	for _, size := range []int{0, 1, 11, 12, 27} {
		require.Error(t, c.Unmarshal(make([]byte, size), &out), "size %d", size)
	}
}

func TestInvalidKeySize(t *testing.T) {
	for _, size := range []int{0, 1, 16, 24, KeySize - 1, KeySize + 1, 64} {
		_, err := New(json.Codec, make([]byte, size))
		require.Error(t, err, "key size %d", size)
	}
}

func TestNilSubCodec(t *testing.T) {
	_, err := New(nil, testKey(1))
	require.Error(t, err)
}

func TestUnsupportedCipher(t *testing.T) {
	_, err := New(json.Codec, testKey(1), WithCipher(CipherSuite(99)))
	require.ErrorIs(t, err, ErrUnsupportedCipher)
}

func TestRandomNoncePerMarshal(t *testing.T) {
	c, err := New(json.Codec, testKey(5), WithCipher(CipherXChaCha20Poly1305))
	require.NoError(t, err)

	value := &struct{ Name string }{Name: "same"}
	first, err := c.Marshal(value)
	require.NoError(t, err)
	second, err := c.Marshal(value)
	require.NoError(t, err)

	require.NotEqual(t, first, second, "each Marshal call must use a fresh nonce")
	require.NotEqual(t, first[:24], second[:24], "nonces must differ")

	var out struct{ Name string }
	require.NoError(t, c.Unmarshal(first, &out))
	require.Equal(t, "same", out.Name)
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
	engine, err := NewAESGCMEngine(testKey(6))
	require.NoError(t, err)

	require.Equal(t, 12, engine.NonceSize())
	require.Equal(t, 16, engine.Overhead())

	nonce := make([]byte, engine.NonceSize())
	ciphertext, err := engine.Encrypt(nonce, []byte("hello"))
	require.NoError(t, err)

	plaintext, err := engine.Decrypt(nonce, ciphertext)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), plaintext)
}

func TestXChaCha20Poly1305Engine(t *testing.T) {
	engine, err := NewXChaCha20Poly1305Engine(testKey(7))
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

func TestEngineInvalidNonceSize(t *testing.T) {
	engines := []CipherEngine{
		mustAESEngine(t),
		mustXChaChaEngine(t),
	}
	for _, engine := range engines {
		badNonce := make([]byte, engine.NonceSize()-1)
		_, err := engine.Encrypt(badNonce, []byte("data"))
		require.Error(t, err)
		_, err = engine.Decrypt(badNonce, []byte("data"))
		require.Error(t, err)
	}
}

func TestEngineAuthFailure(t *testing.T) {
	engines := []CipherEngine{
		mustAESEngine(t),
		mustXChaChaEngine(t),
	}
	for _, engine := range engines {
		nonce := make([]byte, engine.NonceSize())
		_, err := engine.Decrypt(nonce, []byte("not a valid ciphertext"))
		require.ErrorIs(t, err, ErrAuthFailed)
	}
}

func TestNewCipherEngine(t *testing.T) {
	key := testKey(8)

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

	_, err := GenerateNonce(CipherSuite(99))
	require.ErrorIs(t, err, ErrUnsupportedCipher)
}

type errorCodec struct{ err error }

func (e errorCodec) Marshal(interface{}) ([]byte, error) { return nil, e.err }
func (e errorCodec) Unmarshal([]byte, interface{}) error { return e.err }
func (e errorCodec) Name() string                        { return "error" }

func TestSubCodecErrorsPropagate(t *testing.T) {
	sentinel := errors.New("sub-codec failed")

	c, err := New(errorCodec{err: sentinel}, testKey(9))
	require.NoError(t, err)

	_, err = c.Marshal("value")
	require.ErrorIs(t, err, sentinel)

	// Decryption succeeds but decoding fails, so the sub-codec error surfaces.
	writer, err := New(json.Codec, testKey(9))
	require.NoError(t, err)
	encoded, err := writer.Marshal("value")
	require.NoError(t, err)

	var out string
	require.ErrorIs(t, c.Unmarshal(encoded, &out), sentinel)
}

func TestCiphertextLayout(t *testing.T) {
	key := testKey(10)
	c, err := New(json.Codec, key, WithCipher(CipherAES256GCM))
	require.NoError(t, err)

	encoded, err := c.Marshal("payload")
	require.NoError(t, err)
	require.Greater(t, len(encoded), 12+16)

	// Replacing the nonce with zeros must break authentication.
	tampered := bytes.Clone(encoded)
	copy(tampered[:12], make([]byte, 12))

	var out string
	require.ErrorIs(t, c.Unmarshal(tampered, &out), ErrAuthFailed)
}

func mustAESEngine(t *testing.T) CipherEngine {
	t.Helper()
	engine, err := NewAESGCMEngine(testKey(12))
	require.NoError(t, err)
	return engine
}

func mustXChaChaEngine(t *testing.T) CipherEngine {
	t.Helper()
	engine, err := NewXChaCha20Poly1305Engine(testKey(13))
	require.NoError(t, err)
	return engine
}
