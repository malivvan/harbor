package aeadfs

import (
	"fmt"
	"os"

	"github.com/malivvan/storm/internal/encryption"
)

// KeySize is the required master key length in bytes (256 bits).
const KeySize = encryption.KeySize

// RawKeyProvider supplies a fixed 32-byte master key. No key derivation is
// performed: the key is used directly for encryption and decryption.
type RawKeyProvider struct {
	key []byte
}

// NewRawKeyProvider returns a provider that uses key directly. The key must be
// exactly [KeySize] bytes.
func NewRawKeyProvider(key []byte) (*RawKeyProvider, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("aeadfs: key must be %d bytes, got %d", KeySize, len(key))
	}
	// Keep a private copy so later mutations by the caller cannot change the key.
	return &RawKeyProvider{key: append([]byte(nil), key...)}, nil
}

// Key returns a copy of the raw master key.
func (p *RawKeyProvider) Key() ([]byte, error) {
	return append([]byte(nil), p.key...), nil
}

// EnvKeyProvider reads a raw 32-byte master key from an environment variable.
type EnvKeyProvider struct {
	envVar string
}

// NewEnvKeyProvider creates a key provider backed by an environment variable
// holding the raw key bytes.
func NewEnvKeyProvider(envVar string) *EnvKeyProvider {
	return &EnvKeyProvider{envVar: envVar}
}

// Key returns the key stored in the configured environment variable.
func (e *EnvKeyProvider) Key() ([]byte, error) {
	key := []byte(os.Getenv(e.envVar))
	if len(key) == 0 {
		return nil, fmt.Errorf("environment variable %s not set", e.envVar)
	}
	if len(key) != KeySize {
		return nil, fmt.Errorf("key from environment variable must be %d bytes, got %d", KeySize, len(key))
	}
	return key, nil
}
