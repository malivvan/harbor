package fs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/malivvan/storm/fs/absfs"
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
	enc  *encState // nil when encryption is disabled

	mu          sync.Mutex
	filesystems []*FileSystem
}

// Open creates a Store over an already-open bbolt database.
//
// The caller owns db and must keep it open for the lifetime of the Store. The
// Store never closes db. root is an optional parent bucket key for all file
// systems; nil or an empty slice means the database root.
//
// cfg enables optional encryption. A nil config, or one with no encryption
// enabled, creates a plaintext store.
func Open(ctx context.Context, bdb *bolt.DB, key []byte, cfg Config) (*Store, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if bdb == nil {
		return nil, ErrNilParam
	}

	enc, err := resolveConfig(&cfg)
	if err != nil {
		return nil, err
	}

	return &Store{
		db:   bdb,
		root: append([]byte(nil), key...),
		enc:  enc,
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

	if err := s.checkMode(name); err != nil {
		return nil, err
	}

	filesystem, err := newFS(s.db, s.bucketPath(name), s.enc)
	if err != nil {
		return nil, err
	}
	s.track(filesystem)
	return filesystem, nil
}

// OpenWithContentFS creates or opens the named file system with an external
// content filesystem.
func (s *Store) OpenWithContentFS(name string, contentFS absfs.FileSystem) (*FileSystem, error) {
	if s == nil || s.db == nil {
		return nil, ErrNilParam
	}

	if err := s.checkMode(name); err != nil {
		return nil, err
	}

	filesystem, err := newFS(s.db, s.bucketPath(name), s.enc)
	if err != nil {
		return nil, err
	}
	filesystem.contentFS = contentFS
	s.track(filesystem)
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

// checkMode verifies the filesystem's encryption marker against the Store
// configuration before any write happens.
func (s *Store) checkMode(name string) error {
	path := s.bucketPath(name)

	var (
		m      marker
		canary []byte
		found  bool
	)
	err := s.db.View(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		b := newFsBucket(bb)
		if b == nil {
			return nil
		}
		m, canary, found, err = readMarker(b)
		return err
	})
	if err != nil {
		return err
	}

	if found {
		if s.enc == nil {
			return fmt.Errorf("%w: filesystem is encrypted", ErrWrongMode)
		}
		if !m.matches(s.enc) {
			return fmt.Errorf("%w: encryption settings do not match the filesystem", ErrWrongMode)
		}
		return s.enc.verifyCanary(canary)
	}

	if s.enc == nil {
		return nil
	}

	hasData, err := s.hasUserData(path)
	if err != nil {
		return err
	}
	if hasData {
		return fmt.Errorf("%w: filesystem already contains plaintext data", ErrWrongMode)
	}
	return nil
}

// readMarker returns the filesystem encryption marker and its sealed canary.
func readMarker(b *fsBucket) (marker, []byte, bool, error) {
	if b == nil || b.state == nil {
		return marker{}, nil, false, nil
	}
	raw := b.state.Get([]byte(markerKey))
	if raw == nil {
		return marker{}, nil, false, nil
	}
	m, canary, err := decodeMarker(raw)
	if err != nil {
		return marker{}, nil, false, err
	}
	return m, canary, true, nil
}

// hasUserData reports whether a filesystem already stores user data.
func (s *Store) hasUserData(path string) (bool, error) {
	var found bool
	err := s.db.View(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		b := newFsBucket(bb)
		if b == nil {
			return nil
		}

		if err := b.inodes.ForEach(func(key, _ []byte) error {
			if len(key) == 8 && b2i(key) > 1 {
				found = true
			}
			return nil
		}); err != nil {
			return err
		}
		if !found && b.data.Stats().KeyN > 0 {
			found = true
		}
		if !found && b.symlinks.Stats().KeyN > 0 {
			found = true
		}
		if !found && b.chunks != nil && b.chunks.Stats().KeyN > 0 {
			found = true
		}
		return nil
	})
	return found, err
}

// track remembers an opened file system so key rotation can update its
// encryption state.
func (s *Store) track(filesystem *FileSystem) {
	s.mu.Lock()
	s.filesystems = append(s.filesystems, filesystem)
	s.mu.Unlock()
}

// adoptEnc switches the store and every file system it opened to next.
func (s *Store) adoptEnc(next *encState) {
	s.mu.Lock()
	s.enc = next
	for _, filesystem := range s.filesystems {
		filesystem.enc = next
	}
	s.mu.Unlock()
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
