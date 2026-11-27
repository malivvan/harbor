package backup

// objectstore.go — the provider-agnostic object-store contract the snapshotter
// operates over, plus its errors.
//
// The box has exactly one backend for this today: blobstore.Store, which keeps
// objects in one bucket of the single encrypted tempest object store. The
// interface is kept narrow and explicit so the snapshotter, retention and
// restore code stay testable against an in-memory double (memstore_test.go)
// without a database.

import (
	"context"
	"errors"
	"io"
)

// ObjectInfo is the minimal object metadata the snapshotter needs.
type ObjectInfo struct {
	Key  string
	Size int64
	ETag string
}

// ObjectStore is the provider-agnostic object-store contract the snapshotter
// operates over. Implementations must be safe for concurrent use.
//
// List is recursive and returns every object whose key begins with prefix.
// Stat's second return reports whether the object exists (a missing object is
// not an error). Put must be able to accept size == -1 (unknown length) so
// callers may stream.
type ObjectStore interface {
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error
	Delete(ctx context.Context, key string) error
	Stat(ctx context.Context, key string) (ObjectInfo, bool, error)
}

// ErrNotConfigured is returned when a snapshotter is asked to run before the
// tempest store has been opened.
var ErrNotConfigured = errors.New("backup: object store not configured")
