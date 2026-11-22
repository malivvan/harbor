package vec

import (
	"errors"

	"github.com/malivvan/storm/internal/crypto"
)

var (
	// ErrAuthFailed is returned when encrypted data fails authentication.
	ErrAuthFailed = crypto.ErrAuthFailed

	// ErrInvalidKey is returned for a missing or malformed encryption key.
	ErrInvalidKey = crypto.ErrInvalidKey

	// ErrInvalidConfig is returned for an inconsistent encryption config.
	ErrInvalidConfig = crypto.ErrInvalidConfig

	// ErrWrongMode is returned when a store is opened with an encryption mode
	// that does not match what is stored on disk.
	ErrWrongMode = crypto.ErrWrongMode

	// ErrCollectionNotFound is returned when a collection bucket is missing.
	ErrCollectionNotFound = errors.New("vec: collection not found")

	// ErrMetadataNotFound is returned when a collection has no stored metadata.
	ErrMetadataNotFound = errors.New("vec: collection metadata not found")
)
