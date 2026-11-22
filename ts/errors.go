package ts

import (
	"context"
	"errors"

	"github.com/malivvan/storm/internal/encryption"
)

var (
	// ErrNilParam is returned when a required parameter is nil.
	ErrNilParam = errors.New("ts: nil parameter")

	// ErrNilContext is returned when a nil context is passed to an operation.
	ErrNilContext = errors.New("ts: nil context")

	// ErrNotFound is returned when a series or point does not exist.
	ErrNotFound = errors.New("ts: not found")

	// ErrInvalidRange is returned when start is greater than end.
	ErrInvalidRange = errors.New("ts: invalid range")

	// ErrInvalidStep is returned when an aggregation step is not positive.
	ErrInvalidStep = errors.New("ts: invalid step")

	// ErrInvalidRetention is returned for a retention policy with a negative
	// MaxAge or negative MaxPoints.
	ErrInvalidRetention = errors.New("ts: invalid retention")

	// ErrAuthFailed is returned when encrypted data fails authentication.
	ErrAuthFailed = encryption.ErrAuthFailed

	// ErrInvalidKey is returned for a missing or malformed encryption key.
	ErrInvalidKey = encryption.ErrInvalidKey

	// ErrInvalidConfig is returned for an inconsistent encryption config.
	ErrInvalidConfig = encryption.ErrInvalidConfig

	// ErrIncompatibleVersion is returned when the on-disk format revision is
	// not the one this build understands.
	ErrIncompatibleVersion = errors.New("ts: incompatible on-disk format version")

	// ErrWrongMode is returned when a store is opened with an encryption mode
	// that does not match what is stored on disk.
	ErrWrongMode = encryption.ErrWrongMode
)

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	return ctx.Err()
}
