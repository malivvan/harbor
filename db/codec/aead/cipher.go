package aead

import "github.com/malivvan/storm/internal/encryption"

// KeySize is the required key length in bytes (256 bits).
const KeySize = encryption.KeySize

var (
	// ErrAuthFailed is returned when ciphertext fails authentication.
	ErrAuthFailed = encryption.ErrAuthFailed

	// ErrUnsupportedCipher is returned for an unknown cipher suite.
	ErrUnsupportedCipher = encryption.ErrUnsupportedCipher
)

// CipherSuite represents the authenticated-encryption algorithm to use. It is
// an alias for the shared implementation in internal/encryption.
type CipherSuite = encryption.CipherSuite

const (
	// CipherAuto selects AES-256-GCM.
	CipherAuto = encryption.CipherAuto

	// CipherAES256GCM uses AES-256 in Galois/Counter Mode.
	CipherAES256GCM = encryption.CipherAES256GCM

	// CipherXChaCha20Poly1305 uses XChaCha20 with a Poly1305 MAC.
	CipherXChaCha20Poly1305 = encryption.CipherXChaCha20Poly1305
)

// CipherEngine provides AEAD encryption and decryption.
type CipherEngine = encryption.CipherEngine

// AESGCMEngine implements CipherEngine using AES-256-GCM.
type AESGCMEngine = encryption.AESGCMEngine

// XChaCha20Poly1305Engine implements CipherEngine using XChaCha20-Poly1305.
type XChaCha20Poly1305Engine = encryption.XChaCha20Poly1305Engine

// NewAESGCMEngine creates a new AES-256-GCM cipher engine.
func NewAESGCMEngine(key []byte) (*AESGCMEngine, error) {
	return encryption.NewAESGCMEngine(key)
}

// NewXChaCha20Poly1305Engine creates a new XChaCha20-Poly1305 cipher engine.
func NewXChaCha20Poly1305Engine(key []byte) (*XChaCha20Poly1305Engine, error) {
	return encryption.NewXChaCha20Poly1305Engine(key)
}

// NewCipherEngine creates a cipher engine for the given suite and key.
func NewCipherEngine(suite CipherSuite, key []byte) (CipherEngine, error) {
	return encryption.NewCipherEngine(suite, key)
}

// GenerateNonce generates a random nonce for the given cipher suite.
func GenerateNonce(suite CipherSuite) ([]byte, error) {
	return encryption.GenerateNonce(suite)
}

// resolveCipher maps CipherAuto to a concrete suite.
func resolveCipher(suite CipherSuite) (CipherSuite, error) {
	return encryption.Resolve(suite)
}
