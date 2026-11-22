// Package encryption provides the authenticated-encryption primitives shared
// by the ts time-series store, the fs filesystem package and the db and vec
// stores.
//
// Every encrypted store uses AES-256-GCM; keys are always raw 32-byte (256-bit)
// values.
//
// This package also owns the pieces every store shares: key resolution, the
// Keychain (seal/open with rotation fallbacks), the deterministic SIV name
// cipher, the marker framing with its canary, the rotation rewrite helpers and
// the encryption sentinels.
//
// Error convention: the sentinels in this package (ErrAuthFailed, ErrInvalidKey,
// ErrInvalidConfig, ErrWrongMode, ErrEncryptedOrdering) are returned unwrapped.
// Callers may add operation context around them with fmt.Errorf("...: %w", err),
// which keeps errors.Is and errors.As working; the stores in this repository do
// exactly that.
package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"
)

// KeySize is the required key length in bytes (256 bits).
const KeySize = 32

// AESGCMEngine performs authenticated encryption with AES-256-GCM.
type AESGCMEngine struct {
	aead cipher.AEAD
}

// NewAESGCMEngine creates an AES-256-GCM engine for a 32-byte key.
func NewAESGCMEngine(key []byte) (*AESGCMEngine, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("%w: AES-256-GCM requires a %d-byte key, got %d bytes", ErrInvalidKey, KeySize, len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	return &AESGCMEngine{aead: aead}, nil
}

// Encrypt encrypts plaintext using AES-256-GCM.
func (e *AESGCMEngine) Encrypt(nonce, plaintext []byte) ([]byte, error) {
	if len(nonce) != e.NonceSize() {
		return nil, fmt.Errorf("nonce must be %d bytes, got %d", e.NonceSize(), len(nonce))
	}
	return e.aead.Seal(nil, nonce, plaintext, nil), nil
}

// SealInto appends the ciphertext for plaintext to dst, using nonce. It lets
// callers lay out nonce||ciphertext in one buffer.
func (e *AESGCMEngine) SealInto(dst, nonce, plaintext []byte) ([]byte, error) {
	if len(nonce) != e.NonceSize() {
		return nil, fmt.Errorf("nonce must be %d bytes, got %d", e.NonceSize(), len(nonce))
	}
	return e.aead.Seal(dst, nonce, plaintext, nil), nil
}

// Decrypt decrypts ciphertext using AES-256-GCM.
func (e *AESGCMEngine) Decrypt(nonce, ciphertext []byte) ([]byte, error) {
	if len(nonce) != e.NonceSize() {
		return nil, fmt.Errorf("nonce must be %d bytes, got %d", e.NonceSize(), len(nonce))
	}

	plaintext, err := e.aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, ErrAuthFailed
	}
	return plaintext, nil
}

// NonceSize returns the nonce size for AES-GCM (12 bytes).
func (e *AESGCMEngine) NonceSize() int {
	return e.aead.NonceSize()
}

// Overhead returns the authentication tag size (16 bytes).
func (e *AESGCMEngine) Overhead() int {
	return e.aead.Overhead()
}
