package fstree

import "github.com/malivvan/tempest/internal/crypto"

var (
	// ErrAuthFailed is returned when encrypted data fails authentication.
	ErrAuthFailed = crypto.ErrAuthFailed

	// ErrInvalidKey is returned for a missing or malformed encryption key.
	ErrInvalidKey = crypto.ErrInvalidKey

	// ErrInvalidConfig is returned for an inconsistent encryption config.
	ErrInvalidConfig = crypto.ErrInvalidConfig

	// ErrWrongMode is returned when a filesystem is opened with an encryption
	// mode that does not match what is stored on disk.
	ErrWrongMode = crypto.ErrWrongMode
)
