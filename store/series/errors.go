package series

import (
	"context"
	"errors"

	"github.com/malivvan/tempest/internal/crypto"
)

var (
	// ErrNilParam is returned when a required parameter is nil.
	ErrNilParam = errors.New("series: nil parameter")

	// ErrNilContext is returned when a nil context is passed to an operation.
	ErrNilContext = errors.New("series: nil context")

	// ErrNotFound is returned when a series or point does not exist.
	ErrNotFound = errors.New("series: not found")

	// ErrInvalidRange is returned when start is greater than end.
	ErrInvalidRange = errors.New("series: invalid range")

	// ErrInvalidOptions is returned for range options with a negative limit or
	// offset.
	ErrInvalidOptions = errors.New("series: invalid options")

	// ErrInvalidStep is returned when an aggregation step is not positive.
	ErrInvalidStep = errors.New("series: invalid step")

	// ErrInvalidRetention is returned for a retention policy with a negative
	// MaxAge or negative MaxPoints.
	ErrInvalidRetention = errors.New("series: invalid retention")

	// ErrAuthFailed is returned when encrypted data fails authentication.
	ErrAuthFailed = crypto.ErrAuthFailed

	// ErrInvalidKey is returned for a missing or malformed encryption key.
	ErrInvalidKey = crypto.ErrInvalidKey

	// ErrInvalidConfig is returned for an inconsistent encryption config.
	ErrInvalidConfig = crypto.ErrInvalidConfig

	// ErrIncompatibleVersion is returned when the on-disk format revision is
	// not the one this build understands.
	ErrIncompatibleVersion = errors.New("series: incompatible on-disk format version")

	// ErrWrongMode is returned when a store is opened with an encryption mode
	// that does not match what is stored on disk.
	ErrWrongMode = crypto.ErrWrongMode
)

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	return ctx.Err()
}
