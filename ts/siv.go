package ts

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"

	"github.com/malivvan/storm/internal/encryption"
	"golang.org/x/crypto/hkdf"
)

const (
	sivKeySize = 64
	sivInfo    = "storm/ts/siv/v1"
)

// SIVEngine encrypts series names deterministically with AES-SIV so the same
// logical name always maps to the same physical bucket key.
type SIVEngine struct {
	engine *encryption.SIVEngine
}

// newSIVEngine derives a 64-byte SIV key from the master key with HKDF-SHA256.
func newSIVEngine(masterKey []byte) (*SIVEngine, error) {
	if len(masterKey) != encryption.KeySize {
		return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, encryption.KeySize, len(masterKey))
	}

	key := make([]byte, sivKeySize)
	if _, err := io.ReadFull(hkdf.New(sha256.New, masterKey, nil, []byte(sivInfo)), key); err != nil {
		return nil, fmt.Errorf("ts: deriving SIV key: %w", err)
	}

	engine, err := encryption.NewSIVEngine(key)
	if err != nil {
		return nil, err
	}
	return &SIVEngine{engine: engine}, nil
}

// EncryptName returns the base64url-encoded deterministic ciphertext of name.
func (s *SIVEngine) EncryptName(name string) []byte {
	ciphertext, err := s.engine.Encrypt([]byte(name))
	if err != nil {
		// AES-SIV encryption cannot fail with a valid key.
		return nil
	}
	out := make([]byte, base64.RawURLEncoding.EncodedLen(len(ciphertext)))
	base64.RawURLEncoding.Encode(out, ciphertext)
	return out
}

// DecryptName reverses EncryptName.
func (s *SIVEngine) DecryptName(encoded []byte) (string, error) {
	ciphertext := make([]byte, base64.RawURLEncoding.DecodedLen(len(encoded)))
	n, err := base64.RawURLEncoding.Decode(ciphertext, encoded)
	if err != nil {
		return "", fmt.Errorf("ts: decrypting series name: %w", err)
	}

	plain, err := s.engine.Decrypt(ciphertext[:n])
	if err != nil {
		return "", err
	}
	return string(plain), nil
}
