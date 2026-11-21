package fs

import (
	"context"
	"errors"
	"os"
	"path"

	"github.com/absfs/absfs"
	bolt "go.etcd.io/bbolt"
)

var (
	// ErrNilContext is returned when a nil context is passed to an operation.
	ErrNilContext = errors.New("fs: nil context")

	// ErrNilParam is returned when a required parameter is nil.
	ErrNilParam = errors.New("fs: nil parameter")
)

// Store manages named file systems backed by a caller-owned bbolt database.
// Every file system lives under the Store's root bucket.
type Store struct {
	db   *bolt.DB
	root []byte
}

// Open creates a Store over an already-open bbolt database.
//
// The caller owns db and must keep it open for the lifetime of the Store. The
// Store never closes db. root is an optional parent bucket key for all file
// systems; nil or an empty slice means the database root.
func Open(ctx context.Context, db *bolt.DB, root []byte) (*Store, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if db == nil {
		return nil, ErrNilParam
	}

	return &Store{
		db:   db,
		root: append([]byte(nil), root...),
	}, nil
}

// NativeDB returns the underlying bbolt database.
func (s *Store) NativeDB() *bolt.DB {
	if s == nil {
		return nil
	}
	return s.db
}

// Root returns a copy of the Store's root bucket key.
func (s *Store) Root() []byte {
	if s == nil {
		return nil
	}
	return append([]byte(nil), s.root...)
}

// Close releases the Store wrapper. The underlying bbolt database remains open
// and must be closed by its owner.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return ErrNilParam
	}
	return nil
}

// Open creates or opens the named file system under the Store's root. An
// empty name uses the Store's root bucket itself.
func (s *Store) Open(name string) (*FileSystem, error) {
	if s == nil || s.db == nil {
		return nil, ErrNilParam
	}

	filesystem, err := newFS(s.db, s.bucketPath(name))
	if err != nil {
		return nil, err
	}
	return filesystem, nil
}

// OpenWithContentFS creates or opens the named file system with an external
// content filesystem.
func (s *Store) OpenWithContentFS(name string, contentFS absfs.FileSystem) (*FileSystem, error) {
	if s == nil || s.db == nil {
		return nil, ErrNilParam
	}

	filesystem, err := newFS(s.db, s.bucketPath(name))
	if err != nil {
		return nil, err
	}
	filesystem.contentFS = contentFS
	return filesystem, nil
}

// FileSystems returns the names of the file systems stored under the Store's
// root.
func (s *Store) FileSystems() ([]string, error) {
	if s == nil || s.db == nil {
		return nil, ErrNilParam
	}

	var names []string
	err := s.db.View(func(tx *bolt.Tx) error {
		if len(s.root) > 0 {
			root := tx.Bucket(s.root)
			if root == nil {
				return nil
			}
			return root.ForEach(func(name, value []byte) error {
				if value != nil {
					return nil
				}
				if isFSBucket(root.Bucket(name)) {
					names = append(names, string(name))
				}
				return nil
			})
		}

		return tx.ForEach(func(name []byte, bucket *bolt.Bucket) error {
			if isFSBucket(bucket) {
				names = append(names, string(name))
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return names, nil
}

// DeleteFS removes the named file system from the database.
func (s *Store) DeleteFS(name string) error {
	if s == nil || s.db == nil {
		return ErrNilParam
	}
	if name == "" {
		return ErrNilParam
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		var parent *bolt.Bucket
		if len(s.root) > 0 {
			parent = tx.Bucket(s.root)
			if parent == nil {
				return os.ErrNotExist
			}
		}

		var bucket *bolt.Bucket
		if parent == nil {
			bucket = tx.Bucket([]byte(name))
		} else {
			bucket = parent.Bucket([]byte(name))
		}
		if !isFSBucket(bucket) {
			return os.ErrNotExist
		}

		if parent == nil {
			return tx.DeleteBucket([]byte(name))
		}
		return parent.DeleteBucket([]byte(name))
	})
}

func (s *Store) bucketPath(name string) string {
	if len(s.root) == 0 {
		return name
	}
	return path.Join(string(s.root), name)
}

func isFSBucket(bucket *bolt.Bucket) bool {
	if bucket == nil {
		return false
	}
	for _, name := range buckets {
		if bucket.Bucket([]byte(name)) == nil {
			return false
		}
	}
	return true
}
