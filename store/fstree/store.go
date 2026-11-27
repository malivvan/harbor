package fstree

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/malivvan/tempest/pkg/metrics"
	"github.com/malivvan/tempest/store/ledger"
	bolt "go.etcd.io/bbolt"
)

var (
	// ErrNilContext is returned when a nil context is passed to an operation.
	ErrNilContext = errors.New("fstree: nil context")

	// ErrNilParam is returned when a required parameter is nil.
	ErrNilParam = errors.New("fstree: nil parameter")

	// ErrInTransaction reports an operation that cannot run on a store bound
	// to a caller-owned transaction (for example key rotation).
	ErrInTransaction = errors.New("fstree: operation is not available inside an explicit transaction")
)

// Store manages named file systems backed by a caller-owned bbolt database.
// Every file system lives under the Store's root bucket.
type Store struct {
	db *bolt.DB
	// tx, when non-nil, binds every operation to a caller-owned bbolt
	// transaction instead of opening a managed one. A bound Store never
	// commits, rolls back or closes the transaction; see WithTransaction.
	tx *bolt.Tx
	// base points at the store this value is a rooted view of, or nil for a
	// store returned by Open. Views share the handle, encryption state and
	// tracked file systems with their base.
	base *Store
	root [][]byte

	// enc is the optional encryption state; an atomic pointer so a rotation
	// can swap it without racing Open/VerifyAll.
	enc atomic.Pointer[encState]

	// audit receives an entry for every committed write, or nil when the store
	// was opened without audit. It is copied into each FS at Open.
	audit ledger.Sink

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

	cfg, err := cfg.Normalize()
	if err != nil {
		return nil, err
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

// view runs fn against a read-only transaction: the bound transaction when the
// Store came from WithTransaction, or a fresh bbolt view otherwise.
func (s *Store) view(fn func(*bolt.Tx) error) error {
	if s == nil {
		return ErrNilParam
	}
	if s.tx != nil {
		return fn(s.tx)
	}
	return s.db.View(fn)
}

// update runs fn against a write transaction: the bound transaction when the
// Store came from WithTransaction, or a fresh bbolt update otherwise. A bound
// Store never commits; the owner of the transaction does.
func (s *Store) update(fn func(*bolt.Tx) error) error {
	if s == nil {
		return ErrNilParam
	}
	if s.tx != nil {
		return fn(s.tx)
	}
	return s.db.Update(fn)
}

// SetAuditSink replaces the sink that receives committed-write events and
// returns the receiver. It is intended for a transaction-bound view created by
// WithTransaction: the view stages events and the transaction owner records
// them after it commits. It must not be called concurrently with writes on the
// same Store.
func (s *Store) SetAuditSink(sink ledger.Sink) *Store {
	if s != nil {
		s.audit = sink
	}
	return s
}

// WithTransaction returns a Store whose operations run inside tx instead of
// opening their own bbolt transaction. The bound store never commits, rolls
// back or closes tx, and is not tracked by the parent Store. It is used by the
// root package to bind a sub-store to a cross-store transaction.
//
// A bound Store shares the parent's encryption state, audit sink and observer;
// filesystems opened through it are bound to the same transaction. Rotation is
// rejected on a bound Store.
func (s *Store) WithTransaction(tx *bolt.Tx) *Store {
	if s == nil || tx == nil {
		return nil
	}
	return &Store{
		db:    s.db,
		tx:    tx,
		base:  s.rootStore(),
		root:  cloneRoot(s.root),
		audit: s.audit,
		obs:   s.obs,
	}
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
func (s *Store) rootStore() *Store {
	if s != nil && s.base != nil {
		return s.base
	}
	return s
}

// encState returns the encryption state, following a rooted view to its base.
func (s *Store) encState() *encState {
	base := s.rootStore()
	if base == nil {
		return nil
	}
	return base.enc.Load()
}

// WithRoot returns a view of the store rooted at the given bucket path. The
// view shares the handle, encryption state and tracked file systems with the
// receiver. No segments means the database root; pass the conventional root
// explicitly (for example store.FSTREE) to keep it.
func (s *Store) WithRoot(parts ...string) *Store {
	if s == nil {
		return nil
	}
	root := make([][]byte, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		if err := validateBucketName(part); err != nil {
			// An unusable root cannot be repaired here; callers that validate
			// first see the same error from Open. Fall back to the base root.
			root = cloneRoot(s.root)
			break
		}
		root = append(root, []byte(part))
	}
	return &Store{
		db:    s.db,
		tx:    s.tx,
		base:  s.rootStore(),
		root:  root,
		audit: s.audit,
		obs:   s.obs,
	}
}

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

	filesystem, err := newFS(s.db, s.tx, s.bucketPath(name), s.encState(), s.obs)
	if err != nil {
		return nil, err
	}
	filesystem.audit = s.audit
	if s.tx == nil {
		s.track(filesystem)
	}
	return filesystem, nil
}

// OpenReadOnly opens an existing file system without writing or creating
// anything. It is used by backup verification over a read-only database handle.
// The returned FS supports reads only; writes fail with bbolt's
// ErrTxNotWritable.
func (s *Store) OpenReadOnly(name string) (*FS, error) {
	if s == nil || s.db == nil {
		return nil, ErrNilParam
	}
	if err := validateBucketName(name); err != nil {
		return nil, err
	}
	filesystem, err := newFSReadOnly(s.db, s.bucketPath(name), s.encState(), s.obs)
	if err != nil {
		return nil, err
	}
	filesystem.audit = nil
	return filesystem, nil
}

// OpenWithContentFS creates or opens the named file system with an external
// content store. The backend implements the exported ContentFS interface; an
// *FS from this package can be adapted with AsContentFS.
func (s *Store) OpenWithContentFS(name string, contentFS ContentFS) (*FS, error) {
	if s == nil || s.db == nil {
		return nil, ErrNilParam
	}
	if err := validateBucketName(name); err != nil {
		return nil, err
	}

	if err := s.checkMode(name); err != nil {
		return nil, err
	}

	filesystem, err := newFS(s.db, s.tx, s.bucketPath(name), s.encState(), s.obs)
	if err != nil {
		return nil, err
	}
	filesystem.contentFS = contentFS
	filesystem.audit = s.audit
	if s.tx == nil {
		s.track(filesystem)
	}
	return filesystem, nil
}

// FileSystems returns the names of the file systems stored under the Store's
// root.
func (s *Store) FileSystems() ([]string, error) {
	if s == nil || s.db == nil {
		return nil, ErrNilParam
	}

	var names []string
	err := s.view(func(tx *bolt.Tx) error {
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

	err := s.update(func(tx *bolt.Tx) error {
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
		return s.audit.Record(ledger.Entry{
			Store:  ledger.StoreFile,
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
	err := s.view(func(tx *bolt.Tx) error {
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

	enc := s.encState()
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
	err := s.view(func(tx *bolt.Tx) error {
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
	owner := s.rootStore()
	owner.mu.Lock()
	owner.filesystems = append(owner.filesystems, filesystem)
	owner.mu.Unlock()
}

// adoptEnc switches the store and every file system it opened to next.
func (s *Store) adoptEnc(next *encState) {
	owner := s.rootStore()
	owner.mu.Lock()
	owner.enc.Store(next)
	for _, filesystem := range owner.filesystems {
		filesystem.enc.Store(next)
	}
	owner.mu.Unlock()
}

// trackedFS returns the open FS wrapper for name, if this Store opened it.
func (s *Store) trackedFS(name string) *FS {
	path := s.bucketPath(name)
	owner := s.rootStore()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	for _, filesystem := range owner.filesystems {
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
