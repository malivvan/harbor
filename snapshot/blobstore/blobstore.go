// Package blobstore adapts a Harbor blob store to snapshot.ObjectStore.
//
// The snapshotter is written against a prefix-keyed object store: List/Get/Put/
// Delete/Stat all take one key, and snapshot artifacts live under a reserved
// sub-prefix of the application's data prefix. `blob` is bucket-scoped, so this
// adapter fixes the bucket and forwards keys unchanged.
//
// Keeping the adapter separate from the snapshotter means the core stays testable
// against an in-memory double and has no opinion about where bytes live.
package blobstore

import (
	"context"
	"io"

	"github.com/malivvan/harbor/blob"
	"github.com/malivvan/harbor/snapshot"
)

// Store is a snapshot.ObjectStore over one bucket of a blob store.
type Store struct {
	blob   *blob.Store
	bucket string
}

// New returns a Store bound to bucket. A nil blob store is allowed; every method
// then reports an error rather than panicking, so a caller can construct the
// adapter before its database is open.
func New(b *blob.Store, bucket string) *Store {
	return &Store{blob: b, bucket: bucket}
}

// Bucket reports the bucket every key is resolved in.
func (s *Store) Bucket() string { return s.bucket }

// List returns every object under prefix, recursively.
func (s *Store) List(ctx context.Context, prefix string) ([]snapshot.ObjectInfo, error) {
	if s == nil || s.blob == nil {
		return nil, snapshot.ErrNotConfigured
	}
	infos, err := s.blob.List(ctx, s.bucket, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]snapshot.ObjectInfo, 0, len(infos))
	for _, i := range infos {
		out = append(out, snapshot.ObjectInfo{Key: i.Key, Size: i.Size, ETag: i.ETag})
	}
	return out, nil
}

// Get opens key for reading. The caller must Close the reader.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if s == nil || s.blob == nil {
		return nil, snapshot.ErrNotConfigured
	}
	rc, _, err := s.blob.Get(ctx, s.bucket, key)
	return rc, err
}

// Put writes r to key.
func (s *Store) Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error {
	if s == nil || s.blob == nil {
		return snapshot.ErrNotConfigured
	}
	_, err := s.blob.Put(ctx, s.bucket, key, r, size, contentType)
	return err
}

// Delete removes key. A missing object is a no-op.
func (s *Store) Delete(ctx context.Context, key string) error {
	if s == nil || s.blob == nil {
		return snapshot.ErrNotConfigured
	}
	return s.blob.Delete(ctx, s.bucket, key)
}

// Stat reports key's metadata. The bool reports existence.
func (s *Store) Stat(ctx context.Context, key string) (snapshot.ObjectInfo, bool, error) {
	if s == nil || s.blob == nil {
		return snapshot.ObjectInfo{}, false, snapshot.ErrNotConfigured
	}
	info, ok, err := s.blob.Stat(ctx, s.bucket, key)
	if err != nil {
		return snapshot.ObjectInfo{}, false, err
	}
	return snapshot.ObjectInfo{Key: info.Key, Size: info.Size, ETag: info.ETag}, ok, nil
}
