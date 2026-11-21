// Package aead provides authenticated-encryption codecs that wrap another
// codec.MarshalUnmarshaler.
//
// The inner codec serializes a value to bytes; the AEAD layer then encrypts
// those bytes with a raw 32-byte key. Ciphertext is stored as the nonce
// followed by the sealed data (which includes the authentication tag).
//
// Two cipher suites are available: AES-256-GCM (the default) and
// XChaCha20-Poly1305. Both provide confidential, tamper-evident storage and
// use a fresh random nonce for every Marshal call.
//
// The cipher primitives (suite selection, engines, and nonce generation) are
// implemented in the shared internal/encryption package, which is also used by
// the fs/aeadfs filesystem.
package aead

import (
	"errors"
	"fmt"

	"github.com/malivvan/storm/db/codec"
)

const namePrefix = "aead-"

// Codec encrypts values serialized by an inner codec.
type Codec struct {
	sub    codec.MarshalUnmarshaler
	suite  CipherSuite
	engine CipherEngine
}

// Option configures a Codec.
type Option func(*options)

type options struct {
	cipher CipherSuite
}

// WithCipher selects the cipher suite. The default is CipherAuto, which
// resolves to CipherAES256GCM.
func WithCipher(suite CipherSuite) Option {
	return func(o *options) {
		o.cipher = suite
	}
}

// New creates a codec that serializes with sub and encrypts the result with a
// raw 32-byte key. The key must be exactly KeySize bytes.
func New(sub codec.MarshalUnmarshaler, key []byte, opts ...Option) (*Codec, error) {
	if sub == nil {
		return nil, errors.New("aead: sub-codec is nil")
	}

	cfg := options{cipher: CipherAuto}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	suite, err := resolveCipher(cfg.cipher)
	if err != nil {
		return nil, err
	}

	engine, err := NewCipherEngine(suite, key)
	if err != nil {
		return nil, err
	}

	return &Codec{sub: sub, suite: suite, engine: engine}, nil
}

// Cipher returns the resolved cipher suite used by the codec.
func (c *Codec) Cipher() CipherSuite {
	return c.suite
}

// Name returns a stable identifier composed of the cipher suite and the inner
// codec name, for example "aead-aes-256-gcm-json".
func (c *Codec) Name() string {
	return namePrefix + c.suite.String() + "-" + c.sub.Name()
}

// Marshal serializes v with the inner codec and encrypts the result. The
// returned bytes are nonce || ciphertext.
func (c *Codec) Marshal(v interface{}) ([]byte, error) {
	plaintext, err := c.sub.Marshal(v)
	if err != nil {
		return nil, err
	}

	nonce, err := GenerateNonce(c.suite)
	if err != nil {
		return nil, err
	}

	sealed, err := c.engine.Encrypt(nonce, plaintext)
	if err != nil {
		return nil, err
	}

	out := make([]byte, 0, len(nonce)+len(sealed))
	out = append(out, nonce...)
	return append(out, sealed...), nil
}

// Unmarshal decrypts data and decodes it with the inner codec. Authentication
// failures are reported as ErrAuthFailed.
func (c *Codec) Unmarshal(data []byte, v interface{}) error {
	nonceSize := c.engine.NonceSize()
	minimum := nonceSize + c.engine.Overhead()
	if len(data) < minimum {
		return fmt.Errorf("aead: ciphertext too short: got %d bytes, need at least %d", len(data), minimum)
	}

	plaintext, err := c.engine.Decrypt(data[:nonceSize], data[nonceSize:])
	if err != nil {
		return err
	}

	return c.sub.Unmarshal(plaintext, v)
}
