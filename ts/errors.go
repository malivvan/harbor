package ts

import (
	"context"
	"errors"
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
)

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	return ctx.Err()
}
