package kv

import "errors"

var (
	// ErrNilParam is returned when a required parameter is nil.
	ErrNilParam = errors.New("kv: nil parameter")

	// ErrNilContext is returned when a nil context is passed to an operation.
	ErrNilContext = errors.New("kv: nil context")

	// ErrNotFound is returned when a bucket or key does not exist.
	ErrNotFound = errors.New("kv: not found")

	// ErrPtrNeeded is returned when the destination is not a non-nil pointer.
	ErrPtrNeeded = errors.New("kv: destination must be a non-nil pointer")

	// ErrDifferentCodec is returned when a bucket was created with another codec.
	ErrDifferentCodec = errors.New("kv: codec is incompatible with this bucket")
)
