package crypto

import "errors"

// Shared encryption errors. The store packages re-export these as their own
// sentinels (fs.ErrInvalidKey, db.ErrWrongMode, …), so callers can keep matching
// per-package names while the decisions live in one place.
var (
	// ErrAuthFailed is returned when ciphertext fails authentication.
	ErrAuthFailed = errors.New("authentication failed - data may be corrupted or tampered")

	// ErrInvalidKey is returned for a missing or malformed encryption key.
	ErrInvalidKey = errors.New("invalid encryption key")

	// ErrInvalidConfig is returned for an inconsistent encryption config.
	ErrInvalidConfig = errors.New("invalid encryption config")

	// ErrWrongMode is returned when a store is opened with an encryption mode
	// that does not match what is stored on disk.
	ErrWrongMode = errors.New("wrong encryption mode")

	// ErrEncryptedOrdering is returned by ordered or prefix lookups on a store
	// that encrypts keys: ciphertext ordering is meaningless, so these queries
	// fail loud instead of silently returning wrong results.
	ErrEncryptedOrdering = errors.New("ordered and prefix queries are not supported when keys are encrypted")
)
