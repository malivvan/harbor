package fs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/malivvan/harbor/audit"
	"github.com/malivvan/harbor/internal/absfs"
	"github.com/malivvan/harbor/metrics"
	bolt "go.etcd.io/bbolt"
)

// Name is the conventional root bucket the harbor root package uses for the fs
// sub-store. It is not applied implicitly: an empty Config.Root means the
// database root.
const Name = "fs"

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
	root [][]byte

	// enc is the optional encryption state; an atomic pointer so a rotation
	// can swap it without racing Open/VerifyAll.
	enc atomic.Pointer[encState]

	// audit receives an entry for every committed write, or nil when the store
	// was opened without audit. It is copied into each FS at Open.
	audit audit.Sink

	// obs receives operation metrics and spans, or nil when the store was
	// opened without instrumentation. It is copied into each FS at Open.
	obs metrics.Observer

	mu          sync.Mutex
	filesystems []*FS
}

// Open creates a Store over an already-open bbolt database.
//
// The caller owns db and must keep it open for the lifetime of the Store. The
// Store never closes db.
//
// cfg enables optional crypto. cfg.Root is an optional bucket path for all
// file systems; an empty path means the database root. A zero Config, or one
// with no encryption enabled, creates a plaintext store.
func Open(ctx context.Context, bdb *bolt.DB, cfg Config) (*Store, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if bdb == nil {
		return nil, ErrNilParam
	}

	for _, part := range cfg.Root {
		if err := validateBucketName(string(part)); err != nil {
			return nil, err
		}
	}

	enc, err := resolveConfig(cfg)
	if err != nil {
		return nil, err
	}

	store := &Store{
		db:    bdb,
		root:  cloneRoot(cfg.Root),
		audit: cfg.Audit,
		obs:   cfg.Metrics,
	}
	store.enc.Store(enc)
	return store, nil
}

// validateBucketName rejects names that would make the literal-bucket and
// slash-joined path views disagree. An empty name is allowed where the caller
// documents it (the Store root itself).
func validateBucketName(name string) error {
	if name == "" {
		return nil
	}
	if name == "." || name == ".." || strings.Contains(name, "/") {
		return fmt.Errorf("%w: invalid bucket name %q", ErrNilParam, name)
	}
	return nil
}

// Root returns a copy of the Store's root bucket path.
func (s *Store) Root() [][]byte {
	if s == nil {
		return nil
	}
	return cloneRoot(s.root)
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
func (s *Store) Open(name string) (*FS, error) {
	if s == nil || s.db == nil {
		return nil, ErrNilParam
	}
	if err := validateBucketName(name); err != nil {
		return nil, err
	}

	if err := s.checkMode(name); err != nil {
		return nil, err
	}

	filesystem, err := newFS(s.db, s.bucketPath(name), s.enc.Load(), s.obs)
	if err != nil {
		return nil, err
	}
	filesystem.audit = s.audit
	s.track(filesystem)
	return filesystem, nil
}

// OpenWithContentFS creates or opens the named file system with an external
// content filesystem. The backend must implement the module-internal
// absfs.FileSystem interface; an *FS from this package qualifies.
func (s *Store) OpenWithContentFS(name string, contentFS absfs.FileSystem) (*FS, error) {
	if s == nil || s.db == nil {
		return nil, ErrNilParam
	}
	if err := validateBucketName(name); err != nil {
		return nil, err
	}

	if err := s.checkMode(name); err != nil {
		return nil, err
	}

	filesystem, err := newFS(s.db, s.bucketPath(name), s.enc.Load(), s.obs)
	if err != nil {
		return nil, err
	}
	filesystem.contentFS = contentFS
	filesystem.audit = s.audit
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
			root := s.rootBucket(tx)
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
	if err := validateBucketName(name); err != nil {
		return err
	}

	err := s.db.Update(func(tx *bolt.Tx) error {
		var parent *bolt.Bucket
		if len(s.root) > 0 {
			parent = s.rootBucket(tx)
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
	if err != nil {
		return err
	}
	if s.audit != nil {
		return s.audit.Record(audit.Entry{
			Store:  audit.StoreFS,
			Op:     "delete_fs",
			Entity: name,
		})
	}
	return nil
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

	enc := s.enc.Load()
	if found {
		if enc == nil {
			return fmt.Errorf("%w: filesystem is encrypted", ErrWrongMode)
		}
		if !m.matches(enc) {
			return fmt.Errorf("%w: encryption settings do not match the filesystem", ErrWrongMode)
		}
		return enc.verifyCanary(canary, m)
	}

	if enc == nil {
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
func (s *Store) track(filesystem *FS) {
	s.mu.Lock()
	s.filesystems = append(s.filesystems, filesystem)
	s.mu.Unlock()
}

// adoptEnc switches the store and every file system it opened to next.
func (s *Store) adoptEnc(next *encState) {
	s.mu.Lock()
	s.enc.Store(next)
	for _, filesystem := range s.filesystems {
		filesystem.enc.Store(next)
	}
	s.mu.Unlock()
}

// trackedFS returns the open FS wrapper for name, if this Store opened it.
func (s *Store) trackedFS(name string) *FS {
	path := s.bucketPath(name)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, filesystem := range s.filesystems {
		if filesystem.bucket == path {
			return filesystem
		}
	}
	return nil
}

func (s *Store) bucketPath(name string) string {
	parts := make([]string, 0, len(s.root)+1)
	for _, part := range s.root {
		parts = append(parts, string(part))
	}
	if name != "" {
		parts = append(parts, name)
	}
	return path.Join(parts...)
}

// rootBucket traverses the configured root path and returns the bucket, or nil
// when the path is empty or any element is missing.
func (s *Store) rootBucket(tx *bolt.Tx) *bolt.Bucket {
	var b *bolt.Bucket
	for _, name := range s.root {
		if len(name) == 0 {
			continue
		}
		if b == nil {
			b = tx.Bucket(name)
		} else {
			b = b.Bucket(name)
		}
		if b == nil {
			return nil
		}
	}
	return b
}

// cloneRoot returns a deep copy of a bucket path.
func cloneRoot(root [][]byte) [][]byte {
	if root == nil {
		return nil
	}
	out := make([][]byte, len(root))
	for i, part := range root {
		out[i] = append([]byte(nil), part...)
	}
	return out
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
