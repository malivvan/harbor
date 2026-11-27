// Package record is a toolkit for BoltDB that provides indexes, a wide range
// of methods to store and fetch data, an advanced query system, and managed
// transactions with context support.
package record

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/malivvan/tempest/metrics"
	"github.com/malivvan/tempest/store/ledger"
	bolt "go.etcd.io/bbolt"
)

const (
	databaseInfo   = "__database"
	metadataBucket = "__metadata"
	versionKey     = "version"
)

// BucketNamer is an interface that can be implemented by structs to provide
// a custom bucket name. This is essential for runtime-generated types
// (via reflect.StructOf) that have no static type name.
type BucketNamer interface {
	TempestBucketName() string
}

// defaultCodec is the codec used when none is supplied.
var defaultCodec = JSONCodec

// Open initializes tempest on an already-open bbolt database.
//
// The caller owns db and must keep it open for the lifetime of the returned
// Store. tempest never closes db.
//
// cfg configures the store. cfg.Root is an optional bucket path; an empty path
// means the database root. A zero cfg selects the default JSON codec and
// creates a plaintext store. cfg.Codec is the codec used to encode and decode
// entities; a nil Codec selects the default JSON codec.
//
// When cfg enables encryption the store writes an unencrypted marker into the
// __database bucket and verifies it with a sealed canary, so:
//
//   - opening an encrypted store without a Config (or with the wrong settings)
//     fails with ErrWrongMode,
//   - opening an encrypted store with the wrong key fails with ErrAuthFailed,
//   - opening a store that already holds plaintext data with encryption
//     requested fails with ErrWrongMode.
//
// Migrating an existing plaintext store to an encrypted one is not supported
// yet; export and re-import instead.
func Open(ctx context.Context, bdb *bolt.DB, cfg Config) (*Store, error) {
	if err := checkContext(ctx); err != nil {
		return nil, wrapError("open", err)
	}
	if bdb == nil {
		return nil, wrapError("open", ErrNilParam)
	}

	cfg, err := cfg.Normalize()
	if err != nil {
		return nil, wrapError("open", err)
	}
	c := cfg.Codec

	enc, err := resolveConfig(cfg)
	if err != nil {
		return nil, wrapError("open", err)
	}

	s := &Store{bolt: bdb, trace: cfg.Trace, obs: cfg.Metrics}
	s.enc.Store(enc)

	root := cloneBucketPath(cfg.Root)
	n := node{
		s:     s,
		codec: c,
		root:  cloneBucketPath(root),
	}

	s.Node = &n

	if err := s.checkMode(ctx); err != nil {
		return nil, wrapError("open", err)
	}

	if err := s.checkVersion(ctx); err != nil {
		return nil, wrapError("open", err)
	}

	if err := s.checkCapabilities(ctx); err != nil {
		return nil, wrapError("open", err)
	}
	s.expiry = cfg.Expiry || s.caps&CapExpiry != 0

	return s, nil
}

// Store is the wrapper around BoltDB. It contains an instance of BoltDB and uses it to perform all the
// needed operations
type Store struct {
	// The root node that points to the root bucket.
	Node

	// bolt is the underlying bbolt database. It is owned by the caller.
	bolt *bolt.DB

	// base points at the store this value is a rooted view of, or nil for a
	// store returned by Open. Views share the database, encryption state,
	// rotation guard and change hooks with their base.
	base *Store

	// enc is the optional encryption state. It is an atomic pointer so a
	// rotation can swap the keys without racing readers.
	enc atomic.Pointer[encState]

	// rotMu serializes rotation against normal operations. Top-level
	// transactions take RLock; ReEncrypt/RotateAll take Lock across the
	// rewrite and the adoption of the new key state.
	rotMu sync.RWMutex

	// changeMu guards the change hooks. Hooks fire after a write transaction
	// commits, outside the transaction, so a listener may safely read the store.
	changeMu    sync.Mutex
	changeHooks map[uint64]ChangeHook
	nextHookID  uint64

	// trace receives an entry for every committed write, or nil when the store
	// was opened without trace. It is set once at Open and never mutated.
	trace ledger.Sink

	// obs receives operation metrics and spans, or nil when the store was
	// opened without instrumentation. It is set once at Open and never
	// mutated.
	obs metrics.Observer

	// caps is the validated persisted capability bitmask, and expiry reports
	// whether reads must exclude expired records. Both are set at Open.
	caps   uint64
	expiry bool

	// nowMu guards nowFn, the optional clock override used by expiry checks
	// and sweeps. A nil nowFn selects time.Now.
	nowMu sync.RWMutex
	nowFn func() time.Time
}

// Close releases the tempest wrapper. The underlying bbolt database is owned by
// the caller and is not closed.
//
// If the receiver is nil or the underlying bbolt database is nil, Close returns
// ErrNilParam without panicking.
// rootStore returns the store whose shared state this value uses.
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

// rotMuOf returns the rotation guard shared with the base store.
func (s *Store) rotMuOf() *sync.RWMutex {
	base := s.rootStore()
	if base == nil {
		return nil
	}
	return &base.rotMu
}

// changeStore returns the store that owns the change hooks.
func (s *Store) changeStore() *Store {
	return s.rootStore()
}

// WithRoot returns a view of the store rooted at the given bucket path. The
// view shares the bbolt handle, encryption state, rotation guard and change
// hooks with the receiver; it has its own root node and codec selection.
// Passing no segments roots the view at the database root, so callers that
// want to keep the conventional root pass it explicitly (for example store.RECORD).
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
	view := &Store{
		bolt:  s.bolt,
		base:  s.rootStore(),
		trace: s.trace,
		obs:   s.obs,
	}
	view.Node = &node{s: view, codec: s.Codec(), root: root}
	return view
}

func (s *Store) Close() error {
	if s == nil {
		return wrapError("close", ErrNilParam)
	}
	if s.bolt == nil {
		return wrapError("close", ErrNilParam)
	}
	return nil
}

func (s *Store) checkVersion(ctx context.Context) error {
	// Read the encoded value separately so a decode failure can be classified
	// as an incompatible codec while preserving the underlying codec error.
	root, ok := s.Node.(*node)
	if !ok {
		return ErrBadType
	}

	raw, err := root.getBytesRaw(ctx, databaseInfo, versionKey)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return s.Set(ctx, databaseInfo, versionKey, Version)
		}
		return err
	}

	var v string
	err = root.unmarshal(raw, &v)
	if err != nil {
		if errors.Is(err, ErrAuthFailed) {
			return err
		}
		return errors.Join(ErrDifferentCodec, err)
	}
	if v == "" {
		return s.Set(ctx, databaseInfo, versionKey, Version)
	}
	if v != Version {
		return fmt.Errorf("%w: on-disk version %q, this build supports %q", ErrIncompatibleVersion, v, Version)
	}

	return nil
}

// checkMode reconciles the encryption settings of the Store with the marker
// stored on disk. A plaintext store has no marker; an encrypted store always
// has one, which also proves the configured key can decrypt the store.
func (s *Store) checkMode(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	root, ok := s.Node.(*node)
	if !ok {
		return ErrBadType
	}

	m, canary, found, err := root.loadMarker(ctx)
	if err != nil {
		return err
	}

	enc := s.encState()
	if !enc.encrypted() {
		if found {
			return fmt.Errorf("%w: store is encrypted", ErrWrongMode)
		}
		return nil
	}

	if found {
		if !m.matches(enc) {
			return fmt.Errorf("%w: encryption settings do not match the store", ErrWrongMode)
		}
		return enc.verifyCanary(canary, m)
	}

	// No marker: only a brand-new store may be initialized as encrypted.
	hasData, err := root.hasUserData(ctx)
	if err != nil {
		return err
	}
	if hasData {
		return fmt.Errorf("%w: store already contains plaintext data", ErrWrongMode)
	}

	return root.storeMarker(ctx, enc)
}

// toBytes turns an interface into a slice of bytes
func toBytes(key interface{}, c Codec) ([]byte, error) {
	if key == nil {
		return nil, nil
	}
	switch t := key.(type) {
	case []byte:
		return t, nil
	case string:
		return []byte(t), nil
	case int:
		return encodeIntKey(int64(t)), nil
	case int8:
		return encodeIntKey(int64(t)), nil
	case int16:
		return encodeIntKey(int64(t)), nil
	case int32:
		return encodeIntKey(int64(t)), nil
	case int64:
		return encodeIntKey(t), nil
	case uint:
		return encodeUintKey(uint64(t)), nil
	case uint8:
		return encodeUintKey(uint64(t)), nil
	case uint16:
		return encodeUintKey(uint64(t)), nil
	case uint32:
		return encodeUintKey(uint64(t)), nil
	case uint64:
		return encodeUintKey(t), nil
	case float32:
		return encodeFloatKey(float64(t)), nil
	case float64:
		return encodeFloatKey(t), nil
	default:
		return c.Marshal(key)
	}
}

// EncodeKey returns the logical byte encoding tempest uses for a bucket key,
// before the at-rest encryption a store applies. A replicated layer can send
// this encoding to another node and pass it back as a []byte key: toBytes
// treats []byte as already encoded, so the receiver seals the same logical key
// with its own key material.
//
// The encoding is type-sensitive: an int key and a string key that look alike
// produce different bytes. Use the same encoding for writes and reads.
func EncodeKey(key any, codec Codec) ([]byte, error) {
	if codec == nil {
		codec = defaultCodec
	}
	return toBytes(key, codec)
}

// encodeIntKey maps a signed integer to 8 bytes whose byte order matches the
// numeric order (the sign bit is flipped, so negatives sort first).
func encodeIntKey(v int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v)^(1<<63))
	return b[:]
}

// encodeUintKey maps an unsigned integer to 8 big-endian bytes.
func encodeUintKey(v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return b[:]
}

// encodeFloatKey maps a float64 to 8 bytes whose byte order matches the
// numeric order (negative values have all bits inverted, non-negative values
// have the sign bit set).
func encodeFloatKey(v float64) []byte {
	bits := math.Float64bits(v)
	if bits&(1<<63) != 0 {
		bits = ^bits
	} else {
		bits |= 1 << 63
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], bits)
	return b[:]
}

// numbertob encodes an integer counter as 8 big-endian bytes. The layout is
// identical to the previous binary.Write encoding, so existing databases need
// no migration.
func numbertob(v interface{}) ([]byte, error) {
	var n int64
	switch value := v.(type) {
	case int64:
		n = value
	case int:
		n = int64(value)
	case uint64:
		n = int64(value)
	case uint:
		n = int64(value)
	default:
		return nil, fmt.Errorf("numbertob: unsupported counter type %T", v)
	}

	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(n))
	return b[:], nil
}

func numberfromb(raw []byte) (int64, error) {
	if len(raw) != 8 {
		return 0, fmt.Errorf("numberfromb: expected 8 bytes, got %d", len(raw))
	}
	return int64(binary.BigEndian.Uint64(raw)), nil
}
