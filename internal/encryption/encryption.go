// Package encryption provides the authenticated-encryption primitives shared
// by the ts time-series store, the fs filesystem package and the db store.
//
// It defines the cipher-suite selection, the CipherEngine abstraction and the
// AES-256-GCM and XChaCha20-Poly1305 implementations, plus nonce generation.
// Keys are always raw 32-byte (256-bit) values.
//
// This package also owns the pieces every store shares: the key providers and
// key resolution, the Keychain (seal/open with rotation fallbacks), the
// deterministic SIV name cipher, the marker framing with its canary, the
// rotation rewrite helpers and the encryption sentinels.
//
// Error convention: the sentinels in this package (ErrAuthFailed,
// ErrUnsupportedCipher, ErrInvalidKey, ErrInvalidConfig, ErrWrongMode,
// ErrEncryptedOrdering) are returned unwrapped. Callers may add operation
// context around them with fmt.Errorf("...: %w", err), which keeps errors.Is
// and errors.As working; the stores in this repository do exactly that.
package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"

	"golang.org/x/crypto/chacha20poly1305"
)

// KeySize is the required key length in bytes (256 bits).
const KeySize = 32

// CipherSuite represents the authenticated-encryption algorithm to use.
type CipherSuite uint8

const (
	// CipherAuto selects AES-256-GCM.
	CipherAuto CipherSuite = iota

	// CipherAES256GCM uses AES-256 in Galois/Counter Mode.
	CipherAES256GCM

	// CipherXChaCha20Poly1305 uses XChaCha20 with a Poly1305 MAC.
	CipherXChaCha20Poly1305
)

// String returns the string representation of the cipher suite.
func (c CipherSuite) String() string {
	switch c {
	case CipherAuto:
		return "auto"
	case CipherAES256GCM:
		return "aes-256-gcm"
	case CipherXChaCha20Poly1305:
		return "xchacha20-poly1305"
	default:
		return "unknown"
	}
}

// CipherEngine provides AEAD encryption and decryption.
type CipherEngine interface {
	// Encrypt encrypts plaintext with the given nonce.
	Encrypt(nonce, plaintext []byte) ([]byte, error)

	// Decrypt decrypts ciphertext with the given nonce.
	Decrypt(nonce, ciphertext []byte) ([]byte, error)

	// NonceSize returns the size of nonces in bytes.
	NonceSize() int

	// Overhead returns the authentication tag size in bytes.
	Overhead() int
}

// AESGCMEngine implements CipherEngine using AES-256-GCM.
type AESGCMEngine struct {
	aead cipher.AEAD
}

// NewAESGCMEngine creates a new AES-256-GCM cipher engine.
func NewAESGCMEngine(key []byte) (*AESGCMEngine, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("AES-256-GCM requires a %d-byte key, got %d bytes", KeySize, len(key))
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

// XChaCha20Poly1305Engine implements CipherEngine using XChaCha20-Poly1305.
type XChaCha20Poly1305Engine struct {
	aead cipher.AEAD
}

// NewXChaCha20Poly1305Engine creates a new XChaCha20-Poly1305 cipher engine.
func NewXChaCha20Poly1305Engine(key []byte) (*XChaCha20Poly1305Engine, error) {
	if len(key) != chacha20poly1305.KeySize {
		return nil, fmt.Errorf("XChaCha20-Poly1305 requires a %d-byte key, got %d bytes",
			chacha20poly1305.KeySize, len(key))
	}

	// NewX selects XChaCha20-Poly1305, which uses a 24-byte nonce and is
	// safer for random nonces than the 12-byte variant.
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create XChaCha20-Poly1305 cipher: %w", err)
	}

	return &XChaCha20Poly1305Engine{aead: aead}, nil
}

// Encrypt encrypts plaintext using XChaCha20-Poly1305.
func (e *XChaCha20Poly1305Engine) Encrypt(nonce, plaintext []byte) ([]byte, error) {
	if len(nonce) != e.NonceSize() {
		return nil, fmt.Errorf("nonce must be %d bytes, got %d", e.NonceSize(), len(nonce))
	}
	return e.aead.Seal(nil, nonce, plaintext, nil), nil
}

// Decrypt decrypts ciphertext using XChaCha20-Poly1305.
func (e *XChaCha20Poly1305Engine) Decrypt(nonce, ciphertext []byte) ([]byte, error) {
	if len(nonce) != e.NonceSize() {
		return nil, fmt.Errorf("nonce must be %d bytes, got %d", e.NonceSize(), len(nonce))
	}

	plaintext, err := e.aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, ErrAuthFailed
	}
	return plaintext, nil
}

// NonceSize returns the nonce size for XChaCha20-Poly1305 (24 bytes).
func (e *XChaCha20Poly1305Engine) NonceSize() int {
	return e.aead.NonceSize()
}

// Overhead returns the authentication tag size (16 bytes).
func (e *XChaCha20Poly1305Engine) Overhead() int {
	return e.aead.Overhead()
}

// Resolve maps CipherAuto to a concrete cipher suite.
func Resolve(suite CipherSuite) (CipherSuite, error) {
	switch suite {
	case CipherAuto, CipherAES256GCM:
		return CipherAES256GCM, nil
	case CipherXChaCha20Poly1305:
		return CipherXChaCha20Poly1305, nil
	default:
		return CipherAuto, ErrUnsupportedCipher
	}
}

// NewCipherEngine creates a cipher engine for the given suite and key.
func NewCipherEngine(suite CipherSuite, key []byte) (CipherEngine, error) {
	resolved, err := Resolve(suite)
	if err != nil {
		return nil, err
	}

	switch resolved {
	case CipherAES256GCM:
		return NewAESGCMEngine(key)
	case CipherXChaCha20Poly1305:
		return NewXChaCha20Poly1305Engine(key)
	default:
		return nil, ErrUnsupportedCipher
	}
}

// GenerateNonce generates a random nonce for the given cipher suite.
func GenerateNonce(suite CipherSuite) ([]byte, error) {
	resolved, err := Resolve(suite)
	if err != nil {
		return nil, err
	}

	var nonceSize int
	switch resolved {
	case CipherAES256GCM:
		nonceSize = 12 // GCM standard nonce size
	case CipherXChaCha20Poly1305:
		nonceSize = chacha20poly1305.NonceSizeX
	default:
		return nil, ErrUnsupportedCipher
	}

	nonce := make([]byte, nonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}
	return nonce, nil
}
