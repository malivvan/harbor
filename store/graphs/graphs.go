// Package graphs is Harbor's independent property-graph store.
//
// It owns a dedicated root bucket and its own record encoding, so graph data no
// longer depends on the record store's buckets or codec. Each named graph has
// its own bucket with node, edge and version records; values are sealed when
// encryption is enabled. The public graph API lives in the graph package; this
// package supplies its persistence backend and lifecycle.
package graphs

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/malivvan/harbor/graph"
	"github.com/malivvan/harbor/internal/crypto"
	"github.com/malivvan/harbor/pkg/ledger"
	"github.com/malivvan/harbor/pkg/metrics"
	hrecord "github.com/malivvan/harbor/store/record"
	bolt "go.etcd.io/bbolt"
)

// Name is the conventional root bucket the harbor root package uses for the
// graph store. It is not applied implicitly: an empty Config.Root means the
// database root.
const Name = "graphs"

// FormatVersion is the on-disk graph-store format revision this build writes
// and accepts.
const FormatVersion uint64 = 1

const (
	versionKey = "__version__"
	modeKey    = "__mode__"
	canaryKey  = "__canary__"

	canaryPlaintext = "harbor/graphs/v1"
)

// Errors returned by the package.
var (
	ErrNilContext          = errors.New("graphs: nil context")
	ErrNilParam            = errors.New("graphs: nil parameter")
	ErrInvalidConfig       = errors.New("graphs: invalid configuration")
	ErrInvalidName         = errors.New("graphs: invalid graph name")
	ErrIncompatibleVersion = errors.New("graphs: incompatible on-disk format version")
	ErrWrongMode           = errors.New("graphs: store encryption mode does not match")
	ErrReadOnly            = errors.New("graphs: store is read-only")
	ErrInTransaction       = errors.New("graphs: operation is not available inside an explicit transaction")
)

// Config configures a Store.
type Config struct {
	// Root is an optional bucket path for all graphs; empty means the database
	// root. The root package sets ["graphs"].
	Root [][]byte

	// Secret is the 32-byte key used when EncryptValues is enabled.
	Secret []byte

	// EncryptValues seals node, edge and version records.
	EncryptValues bool

	// Audit, when non-nil, receives one entry per committed graph record write.
	Audit ledger.Sink

	// Metrics, when non-nil, receives operation counters.
	Metrics metrics.Observer
}

// encState holds the resolved encryption settings.
type encState struct {
	kc *crypto.Keychain
}

// Store manages named graphs backed by a caller-owned bbolt database.
type Store struct {
	db *bolt.DB
	// tx, when non-nil, binds every operation to a caller-owned transaction.
	tx *bolt.Tx
	// base points at the store this value is a rooted view of, or nil.
	base *Store
	root [][]byte
	enc  atomic.Pointer[encState]

	audit ledger.Sink
	obs   metrics.Observer

	closed   atomic.Bool
	readOnly bool
}

// Open creates a Store over an already-open bbolt database. The caller owns db
// and must keep it open for the lifetime of the Store.
func Open(ctx context.Context, bdb *bolt.DB, cfg Config) (*Store, error) {
	return open(ctx, bdb, cfg, false)
}

// OpenReadOnly validates and reads existing graphs without writing.
func OpenReadOnly(ctx context.Context, bdb *bolt.DB, cfg Config) (*Store, error) {
	return open(ctx, bdb, cfg, true)
}

func open(ctx context.Context, bdb *bolt.DB, cfg Config, readOnly bool) (*Store, error) {
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
	s := &Store{
		db:       bdb,
		root:     cloneRoot(cfg.Root),
		audit:    cfg.Audit,
		obs:      cfg.Metrics,
		readOnly: readOnly,
	}
	s.enc.Store(enc)
	if err := s.ensureMeta(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func resolveEnc(cfg Config) (*encState, error) {
	if !cfg.EncryptValues {
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
	return &encState{kc: kc}, nil
}

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

func validateName(name string) error {
	if name == "" || name == "." || name == ".." || strings.Contains(name, "/") {
		return fmt.Errorf("%w: %q", ErrInvalidName, name)
	}
	for i := 0; i < len(name); i++ {
		if name[i] == 0 {
			return fmt.Errorf("%w: %q", ErrInvalidName, name)
		}
	}
	if strings.HasPrefix(name, "__") {
		return fmt.Errorf("%w: %q is reserved", ErrInvalidName, name)
	}
	return nil
}

// rootStore returns the store whose shared state this value uses.
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

// Root returns a copy of the store's root bucket path.
func (s *Store) Root() [][]byte {
	if s == nil {
		return nil
	}
	return cloneRoot(s.root)
}

// WithRoot returns a view rooted at the given bucket path. It shares the
// handle, encryption state and audit sink with the receiver.
func (s *Store) WithRoot(parts ...string) *Store {
	if s == nil {
		return nil
	}
	root := make([][]byte, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		if err := validateName(part); err != nil {
			root = cloneRoot(s.root)
			break
		}
		root = append(root, []byte(part))
	}
	return &Store{
		db:       s.db,
		tx:       s.tx,
		base:     s.rootStore(),
		root:     root,
		audit:    s.audit,
		obs:      s.obs,
		readOnly: s.readOnly,
	}
}

// WithTransaction returns a Store bound to tx.
func (s *Store) WithTransaction(tx *bolt.Tx) *Store {
	if s == nil || tx == nil {
		return nil
	}
	return &Store{
		db:       s.db,
		tx:       tx,
		base:     s.rootStore(),
		root:     cloneRoot(s.root),
		audit:    s.audit,
		obs:      s.obs,
		readOnly: s.readOnly,
	}
}

// SetAuditSink replaces the sink that receives committed-write entries and
// returns the receiver. It is intended for a transaction-bound view.
func (s *Store) SetAuditSink(sink ledger.Sink) *Store {
	if s != nil {
		s.audit = sink
	}
	return s
}

// checkOpen reports whether the store can serve requests.
func (s *Store) checkOpen() error {
	if s == nil {
		return ErrNilParam
	}
	if s.rootStore().closed.Load() {
		return errors.New("graphs: store is closed")
	}
	if s.db == nil {
		return ErrNilParam
	}
	return nil
}

// Close marks a standalone store closed. It never closes the shared handle; a
// bound view is never closed through this method.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.rootStore().closed.Store(true)
	return nil
}

// Flush is a no-op: graph records are written synchronously.
func (s *Store) Flush() error { return s.checkOpen() }

// view runs fn against the bound transaction, or a fresh read transaction.
func (s *Store) view(ctx context.Context, fn func(*bolt.Tx) error) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if s.tx != nil {
		return fn(s.tx)
	}
	return s.db.View(fn)
}

// update runs fn against the bound transaction, or a fresh write transaction.
func (s *Store) update(ctx context.Context, fn func(*bolt.Tx) error) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if s.readOnly && s.tx == nil {
		return ErrReadOnly
	}
	if s.tx != nil {
		return fn(s.tx)
	}
	return s.db.Update(fn)
}

// rootBucket traverses the configured root, or returns nil.
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

// createRootBucket creates the configured root and returns it.
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
	if b == nil {
		return nil, ErrInvalidConfig
	}
	return b, nil
}

// ensureMeta validates the version, mode and canary, creating them for a new
// store.
func (s *Store) ensureMeta(ctx context.Context) error {
	if s.readOnly {
		return s.view(ctx, func(tx *bolt.Tx) error {
			root := s.rootBucket(tx)
			if root == nil {
				return fmt.Errorf("%w: missing root bucket", ErrIncompatibleVersion)
			}
			return s.checkMeta(root, true)
		})
	}
	return s.update(ctx, func(tx *bolt.Tx) error {
		root, err := s.createRootBucket(tx)
		if err != nil {
			return err
		}
		return s.checkMeta(root, false)
	})
}

func (s *Store) checkMeta(root *bolt.Bucket, readOnly bool) error {
	raw := root.Get([]byte(versionKey))
	if raw == nil {
		if readOnly {
			return fmt.Errorf("%w: missing version record", ErrIncompatibleVersion)
		}
		var buf [8]byte
		for i := range buf {
			buf[i] = byte(FormatVersion >> (8 * i))
		}
		if err := root.Put([]byte(versionKey), buf[:]); err != nil {
			return err
		}
	} else {
		if len(raw) != 8 {
			return fmt.Errorf("%w: malformed version record", ErrIncompatibleVersion)
		}
		var v uint64
		for i := len(raw) - 1; i >= 0; i-- {
			v = v<<8 | uint64(raw[i])
		}
		if v != FormatVersion {
			return fmt.Errorf("%w: graph format %d, this build supports %d", ErrIncompatibleVersion, v, FormatVersion)
		}
	}

	want := byte(0)
	enc := s.encState()
	if enc != nil {
		want = 1
	}
	mode := root.Get([]byte(modeKey))
	if mode == nil {
		if readOnly {
			return ErrWrongMode
		}
		if err := root.Put([]byte(modeKey), []byte{want}); err != nil {
			return err
		}
	} else if len(mode) != 1 || mode[0] != want {
		return fmt.Errorf("%w: stored mode %#x, configured %#x", ErrWrongMode, mode[0], want)
	}

	canary := root.Get([]byte(canaryKey))
	if enc != nil {
		if canary == nil {
			if readOnly {
				return ErrWrongMode
			}
			sealed, err := enc.kc.Seal([]byte(canaryPlaintext + "/0x01"))
			if err != nil {
				return err
			}
			if err := root.Put([]byte(canaryKey), sealed); err != nil {
				return err
			}
			return nil
		}
		plain, err := enc.kc.Open(canary)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrWrongMode, err)
		}
		if string(plain) != canaryPlaintext+"/0x01" {
			return ErrWrongMode
		}
	}
	return nil
}

// seal seals plain when value encryption is enabled.
func (s *Store) seal(plain []byte) ([]byte, error) {
	enc := s.encState()
	if enc == nil {
		return plain, nil
	}
	return enc.kc.Seal(plain)
}

// openValue opens raw when value encryption is enabled.
func (s *Store) openValue(raw []byte) ([]byte, error) {
	enc := s.encState()
	if enc == nil {
		return raw, nil
	}
	return enc.kc.Open(raw)
}

// Open opens or creates the named graph and returns the graph API value.
func (s *Store) Open(name string, opts ...graph.Option) (*graph.Graph, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := validateName(name); err != nil {
		return nil, err
	}
	if !s.readOnly && s.tx == nil {
		if err := s.update(context.Background(), func(tx *bolt.Tx) error {
			return s.ensureGraphBucket(tx, name)
		}); err != nil {
			return nil, err
		}
	}
	return graph.NewStorage(&graphStorage{store: s, name: name}, opts...), nil
}

// ensureGraphBucket creates the four record buckets for a graph.
func (s *Store) ensureGraphBucket(tx *bolt.Tx, name string) error {
	root, err := s.createRootBucket(tx)
	if err != nil {
		return err
	}
	gb, err := root.CreateBucketIfNotExists([]byte(name))
	if err != nil {
		return err
	}
	for _, bucket := range []string{nodesBucket, nodeVersionsBucket, edgesBucket, edgeVersionsBucket} {
		if _, err := gb.CreateBucketIfNotExists([]byte(bucket)); err != nil {
			return err
		}
	}
	return nil
}

// Names lists the graph names in the store.
func (s *Store) Names(ctx context.Context) ([]string, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	var names []string
	err := s.view(ctx, func(tx *bolt.Tx) error {
		root := s.rootBucket(tx)
		if root == nil {
			return nil
		}
		return root.ForEach(func(k, v []byte) error {
			if v == nil && !strings.HasPrefix(string(k), "__") {
				names = append(names, string(k))
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return names, nil
}

// MigrateFromRecord copies a graph that was stored in the legacy record-backed
// layout into the named independent graph. It copies live records and the full
// closed-version history, and is idempotent.
//
// The legacy data is read with graph.New, which pins the original bucket names
// and codec; this package never reinterprets it in place.
func (s *Store) MigrateFromRecord(ctx context.Context, name string, rec *hrecord.Store, prefix ...string) error {
	if rec == nil {
		return ErrNilParam
	}
	src := graph.New(rec, graph.WithPrefix(prefix...))
	dst, err := s.Open(name)
	if err != nil {
		return err
	}
	return graph.Copy(ctx, src, dst)
}

// digestOf returns the SHA-256 of data, or nil when empty.
func digestOf(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	sum := sha256.Sum256(data)
	return sum[:]
}
