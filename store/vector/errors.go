package vector

import (
	"errors"

	"github.com/malivvan/tempest/internal/crypto"
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
	ErrCollectionNotFound = errors.New("vector: collection not found")

	// ErrMetadataNotFound is returned when a collection has no stored metadata.
	ErrMetadataNotFound = errors.New("vector: collection metadata not found")

	// ErrPointNotFound is returned when a point ID is not present.
	ErrPointNotFound = errors.New("vector: point not found")

	// ErrMixedModels reports a collection whose points were embedded with more
	// than one model and which was not opened with AllowMixedModels.
	ErrMixedModels = errors.New("vector: collection contains points from mixed embedding models")

	// ErrInTransaction reports an operation that cannot run on a store bound to
	// a caller-owned transaction (for example key rotation).
	ErrInTransaction = errors.New("vector: operation is not available inside an explicit transaction")

	// ErrInvalidOptions is returned for invalid list options or a nil iteration
	// callback.
	ErrInvalidOptions = errors.New("vector: invalid options")

	// ErrStopIteration can be returned from a ForEachPoint callback to stop the
	// iteration without reporting an error.
	ErrStopIteration = errors.New("vector: stop iteration")
)
