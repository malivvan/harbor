package aeadfs

import "github.com/malivvan/storm/internal/encryption"

// CipherEngine is the AEAD engine interface used by the package. It is an alias
// for the shared implementation in internal/encryption.
type CipherEngine = encryption.CipherEngine

// AESGCMEngine encrypts and decrypts with AES-256-GCM.
type AESGCMEngine = encryption.AESGCMEngine

// XChaCha20Poly1305Engine encrypts and decrypts with XChaCha20-Poly1305.
type XChaCha20Poly1305Engine = encryption.XChaCha20Poly1305Engine

// NewAESGCMEngine creates a new AES-256-GCM engine. The key must be KeySize bytes.
func NewAESGCMEngine(key []byte) (*AESGCMEngine, error) {
	return encryption.NewAESGCMEngine(key)
}

// NewXChaCha20Poly1305Engine creates a new XChaCha20-Poly1305 engine. The key
// must be KeySize bytes.
func NewXChaCha20Poly1305Engine(key []byte) (*XChaCha20Poly1305Engine, error) {
	return encryption.NewXChaCha20Poly1305Engine(key)
}

// NewCipherEngine creates the engine for the given cipher suite.
func NewCipherEngine(cipher CipherSuite, key []byte) (CipherEngine, error) {
	return encryption.NewCipherEngine(cipher, key)
}

// GenerateNonce generates a random nonce for the given cipher suite.
func GenerateNonce(cipher CipherSuite) ([]byte, error) {
	return encryption.GenerateNonce(cipher)
}
