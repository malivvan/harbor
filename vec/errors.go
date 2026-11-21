package vec

import "github.com/malivvan/storm/internal/encryption"

var (
	// ErrAuthFailed is returned when encrypted data fails authentication.
	ErrAuthFailed = encryption.ErrAuthFailed

	// ErrUnsupportedCipher is returned for an unknown cipher suite.
	ErrUnsupportedCipher = encryption.ErrUnsupportedCipher

	// ErrInvalidKey is returned for a missing or malformed encryption key.
	ErrInvalidKey = encryption.ErrInvalidKey

	// ErrInvalidConfig is returned for an inconsistent encryption config.
	ErrInvalidConfig = encryption.ErrInvalidConfig

	// ErrWrongMode is returned when a store is opened with an encryption mode
	// that does not match what is stored on disk.
	ErrWrongMode = encryption.ErrWrongMode
)
