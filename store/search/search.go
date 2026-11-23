// Package search is a full-text index for Harbor records.
//
// An index stores a document per id: the text is tokenized into terms, each
// term keeps a posting list of document ids, and each document keeps its length
// and term frequencies so BM25 can be computed without re-reading the text.
// Upserting an existing id replaces its postings atomically; deleting removes
// them and updates the corpus statistics.
//
// Storage lives under its own root bucket (`fts/` in the root store), with an
// independent format version. Values (documents, posting lists and metadata)
// are sealed when value encryption is enabled, and term keys can optionally be
// encrypted deterministically; see Config.
package search

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/malivvan/harbor/internal/crypto"
	"github.com/malivvan/harbor/pkg/metrics"
	bolt "go.etcd.io/bbolt"
)

// FormatVersion is the on-disk full-text format revision this build writes and
// accepts. Any other value fails the open with ErrIncompatibleVersion.
const FormatVersion uint64 = 1

// Default BM25 parameters.
const (
	DefaultK1 = 1.2
	DefaultB  = 0.75
)

// Errors returned by the package.
var (
	// ErrNilContext is returned when a nil context is passed to an operation.
	ErrNilContext = errors.New("search: nil context")

	// ErrNilParam is returned when a required parameter is nil.
	ErrNilParam = errors.New("search: nil parameter")

	// ErrInvalidConfig reports an unusable configuration.
	ErrInvalidConfig = errors.New("search: invalid configuration")

	// ErrInvalidName reports an empty or malformed index name.
	ErrInvalidName = errors.New("search: invalid index name")

	// ErrIncompatibleVersion reports an index written by another format.
	ErrIncompatibleVersion = errors.New("search: incompatible on-disk format version")

	// ErrWrongMode reports an index opened with the wrong encryption mode.
	ErrWrongMode = errors.New("search: index encryption mode does not match")

	// ErrInTransaction reports an operation that cannot run on a store bound to
	// a caller-owned transaction (for example key rotation).
	ErrInTransaction = errors.New("search: operation is not available inside an explicit transaction")

	// ErrIndexNotFound reports a missing index.
	ErrIndexNotFound = errors.New("search: index not found")
)

// Config configures a Store or an Index.
type Config struct {
	// Root is an optional bucket path for all indexes; empty means the
	// database root. The root package sets ["fts"].
	Root [][]byte

	// Tokenizer splits text into terms. A nil Tokenizer selects Words.
	Tokenizer Tokenizer

	// K1 and B are the BM25 parameters. Zero selects the defaults. They are
	// fixed when an index is created and checked on reopen.
	K1 float64
	B  float64

	// Secret is the 32-byte key used when value or token encryption is
	// enabled. A nil Secret means plaintext.
	Secret []byte

	// EncryptValues seals document and posting-list values.
	EncryptValues bool

	// EncryptTokens encrypts term bucket keys deterministically. It hides the
	// vocabulary but still leaks term equality and result sizes; it is off by
	// default.
	EncryptTokens bool

	// Metrics, when non-nil, receives operation counters.
	Metrics metrics.Observer
}

// Store manages named full-text indexes backed by a caller-owned bbolt
// database.
type Store struct {
	db *bolt.DB
	// tx, when non-nil, binds every operation to a caller-owned transaction.
	// See WithTransaction.
	tx *bolt.Tx
	// base points at the store this value is a rooted view of, or nil for a
	// store returned by Open. Views share the handle and encryption state.
	base     *Store
	root     [][]byte
	enc      atomic.Pointer[encState]
	obs      metrics.Observer
	closed   atomic.Bool
	readOnly bool
}

// encState holds the resolved encryption settings for one store.
type encState struct {
	kc        *crypto.Keychain
	siv       *crypto.SIVNameCipher
	encValues bool
	encTokens bool
}

// Open creates a Store over an already-open bbolt database. The caller owns db
// and must keep it open for the lifetime of the Store; the Store never closes
// it.
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
		if err := validateName(string(part)); err != nil {
			return nil, err
		}
	}
	enc, err := resolveEnc(cfg)
	if err != nil {
		return nil, err
	}
	store := &Store{
		db:   bdb,
		root: cloneRoot(cfg.Root),
		obs:  cfg.Metrics,
	}
	store.enc.Store(enc)
	return store, nil
}

// resolveEnc builds the encryption state.
func resolveEnc(cfg Config) (*encState, error) {
	if !cfg.EncryptValues && !cfg.EncryptTokens {
		return nil, nil
	}
	key, err := crypto.ResolveKey(cfg.Secret)
	if err != nil {
		return nil, err
	}
	kc, err := crypto.NewKeychain([][]byte{key})
	if err != nil {
		return nil, err
	}
	siv, err := crypto.NewSIVNameCipher(key, "harbor/fts/v1")
	if err != nil {
		return nil, err
	}
	return &encState{kc: kc, siv: siv, encValues: cfg.EncryptValues, encTokens: cfg.EncryptTokens}, nil
}

// cloneRoot copies a bucket path.
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

// validateName rejects names that would make a bucket path ambiguous. The empty
// name is allowed only where the caller documents it (the store root).
func validateName(name string) error {
	if name == "" {
		return nil
	}
	if name == "." || name == ".." {
		return fmt.Errorf("%w: %q", ErrInvalidName, name)
	}
	for i := 0; i < len(name); i++ {
		if name[i] == '/' || name[i] == 0 {
			return fmt.Errorf("%w: %q", ErrInvalidName, name)
		}
	}
	return nil
}

// view runs fn against the bound transaction, or a fresh read transaction.
func (s *Store) view(fn func(*bolt.Tx) error) error {
	if s == nil {
		return ErrNilParam
	}
	if s.tx != nil {
		return fn(s.tx)
	}
	return s.db.View(fn)
}

// update runs fn against the bound transaction, or a fresh write transaction.
func (s *Store) update(fn func(*bolt.Tx) error) error {
	if s == nil {
		return ErrNilParam
	}
	if s.tx != nil {
		return fn(s.tx)
	}
	return s.db.Update(fn)
}

// OpenReadOnly creates a Store that only reads. Every operation uses a read
// transaction and index opens validate existing buckets instead of creating
// them. It is used by backup verification over a read-only database handle.
func OpenReadOnly(ctx context.Context, bdb *bolt.DB, cfg Config) (*Store, error) {
	s, err := Open(ctx, bdb, cfg)
	if err != nil {
		return nil, err
	}
	s.readOnly = true
	return s, nil
}

// WithTransaction returns a Store whose operations run inside tx. The bound
// store never commits, rolls back or closes tx and is not tracked by the
// parent.
func (s *Store) WithTransaction(tx *bolt.Tx) *Store {
	if s == nil || tx == nil {
		return nil
	}
	return &Store{
		db:       s.db,
		tx:       tx,
		base:     s.rootStore(),
		root:     cloneRoot(s.root),
		obs:      s.obs,
		readOnly: s.readOnly,
	}
}

// encState returns the current encryption state.
func (s *Store) rootStore() *Store {
	if s != nil && s.base != nil {
		return s.base
	}
	return s
}

func (s *Store) encState() *encState {
	base := s.rootStore()
	if base == nil {
		return nil
	}
	return base.enc.Load()
}

// WithRoot returns a view of the store rooted at the given bucket path. The
// view shares the handle and encryption state with the receiver. No segments
// means the database root; pass the conventional root explicitly (for example
// "fts") to keep it.
func (s *Store) WithRoot(parts ...string) *Store {
	if s == nil {
		return nil
	}
	root := make([][]byte, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		root = append(root, []byte(part))
	}
	return &Store{
		db:       s.db,
		tx:       s.tx,
		base:     s.rootStore(),
		root:     root,
		obs:      s.obs,
		readOnly: s.readOnly,
	}
}

// rootBucket traverses the configured root, or returns nil when it is missing.
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

// createRootBucket creates the configured root and returns its innermost
// bucket.
func (s *Store) createRootBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	var b *bolt.Bucket
	for _, name := range s.root {
		if len(name) == 0 {
			continue
		}
		var err error
		if b == nil {
			b, err = tx.CreateBucketIfNotExists(name)
		} else {
			b, err = b.CreateBucketIfNotExists(name)
		}
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

// indexBucket returns the bucket for name, or nil when it does not exist.
func (s *Store) indexBucket(tx *bolt.Tx, name string) (*bolt.Bucket, error) {
	if len(s.root) > 0 {
		root := s.rootBucket(tx)
		if root == nil {
			return nil, nil
		}
		return root.Bucket([]byte(name)), nil
	}
	return tx.Bucket([]byte(name)), nil
}

// createIndexBucket creates the bucket for name.
func (s *Store) createIndexBucket(tx *bolt.Tx, name string) (*bolt.Bucket, error) {
	if len(s.root) > 0 {
		root, err := s.createRootBucket(tx)
		if err != nil {
			return nil, err
		}
		if root == nil {
			return nil, fmt.Errorf("%w: root path is empty", ErrInvalidConfig)
		}
		return root.CreateBucketIfNotExists([]byte(name))
	}
	return tx.CreateBucketIfNotExists([]byte(name))
}

// seal seals plain when value encryption is enabled.
func (s *Store) seal(plain []byte) ([]byte, error) {
	enc := s.encState()
	if enc == nil || !enc.encValues {
		return plain, nil
	}
	return enc.kc.Seal(plain)
}

// openValue opens raw when value encryption is enabled.
func (s *Store) openValue(raw []byte) ([]byte, error) {
	enc := s.encState()
	if enc == nil || !enc.encValues {
		return raw, nil
	}
	return enc.kc.Open(raw)
}

// termKey maps a term to its bucket key, encrypting it deterministically when
// requested.
func (s *Store) termKey(term string) ([]byte, error) {
	enc := s.encState()
	if enc == nil || !enc.encTokens {
		return []byte(term), nil
	}
	return enc.siv.SealName([]byte(term))
}
