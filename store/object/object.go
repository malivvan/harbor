// Package object is tempest's independent flat object store.
//
// A bucket is a namespace and a key is a slash-separated name inside it. The
// pair maps to exactly one record in the object store's own root bucket, so the
// object store works without a file system and never touches the fstree store's
// buckets. Keys containing a ".." segment are rejected rather than rewritten,
// so a key can never address a sibling bucket.
//
// The API is the blob API the earlier fstree store shipped before this store
// existed: Put
// installs a complete object atomically, Get returns a reader, Delete is a
// no-op for a missing key, Move relocates one key, and Stat/List report the
// size and ETag. Existing file-backed blobs are not abandoned: the fstree store
// still holds them and MigrateFromFile copies a bucket into this store.
package object

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/malivvan/tempest/internal/compress"
	"github.com/malivvan/tempest/internal/crypto"
	"github.com/malivvan/tempest/metrics"
	"github.com/malivvan/tempest/store/fstree"
	"github.com/malivvan/tempest/store/ledger"
	bolt "go.etcd.io/bbolt"
)

// FormatVersion is the on-disk object-store format revision this build writes
// and accepts.
const FormatVersion uint64 = 1

// FormatVersionChunked is written when objects may be stored as chunks. Older
// builds refuse it, so they cannot read a chunked object as an empty blob.
const FormatVersionChunked uint64 = 2

// DefaultChunkSize is the plaintext chunk size used by PutStream.
const DefaultChunkSize = 1 << 20

const (
	versionKey = "__version__"
	modeKey    = "__mode__"
	canaryKey  = "__canary__"

	// modeDedup records that objects are stored content-addressed.
	modeDedup byte = 1 << 1

	blobsBucketName     = "__blobs__"
	blobRefsBucketName  = "__blobs_refs__"
	chunksBucketName    = "__chunks__"
	chunkRefsBucketName = "__chunk_refs__"

	canaryPlaintext = "tempest/object/v1"
)

// Errors returned by the package.
var (
	// ErrNoStore reports an operation on a Store without a database.
	ErrNoStore = errors.New("object: no store configured")
	// ErrInvalidBucket reports an empty or unusable bucket name.
	ErrInvalidBucket = errors.New("object: invalid bucket")
	// ErrInvalidKey reports an empty key or one containing a ".." segment.
	ErrInvalidKey = errors.New("object: invalid key")
	// ErrIncompatibleVersion reports a store written by another format.
	ErrIncompatibleVersion = errors.New("object: incompatible on-disk format version")
	// ErrWrongMode reports a store opened with the wrong encryption mode.
	ErrWrongMode = errors.New("object: store encryption mode does not match")
	// ErrReadOnly reports a write on a read-only verification store.
	ErrReadOnly = errors.New("object: store is read-only")
	// ErrInvalidConfig reports an unusable configuration, such as a rotation
	// key of the wrong length.
	ErrInvalidConfig = errors.New("object: invalid configuration")
)

// Config configures a Store.
type Config struct {
	// Root is an optional bucket path for the object store; empty means the
	// database root. The root package sets ["object"].
	Root [][]byte

	// Secret is the 32-byte key used when EncryptValues is enabled.
	Secret []byte

	// EncryptValues seals object records.
	EncryptValues bool

	// Compress Zstd-compresses object records (content and metadata) on the
	// plaintext side of encryption. It is a write-time preference: reads
	// auto-detect the envelope, so a store written compressed stays readable
	// with Compress disabled. It does not require encryption.
	Compress bool

	// CompressLevel selects the Zstandard level (0 selects the default).
	CompressLevel int

	// MinCompress skips compressing records smaller than this many bytes.
	MinCompress int

	// Dedup stores object content by its SHA-256, so identical objects across
	// keys and buckets are stored once and reference-counted. The layout is
	// recorded in the store's mode byte and a mismatch fails closed.
	Dedup bool

	// Trace, when non-nil, receives one entry per committed object write.
	Trace ledger.Sink

	// Metrics, when non-nil, receives operation counters.
	Metrics metrics.Observer
}

// encState holds the resolved encryption settings.
type encState struct {
	kc   *crypto.Keychain
	comp *compress.Codec
}

// encrypted reports whether a real cipher is configured. A state can exist for
// compression alone, with no keychain.
func (e *encState) encrypted() bool { return e != nil && e.kc != nil }

// Store is a flat object namespace backed by one bbolt database.
type Store struct {
	db *bolt.DB
	// tx, when non-nil, binds every operation to a caller-owned transaction.
	tx *bolt.Tx
	// base points at the store this value is a rooted view of, or nil.
	base *Store
	root [][]byte
	enc  atomic.Pointer[encState]

	trace ledger.Sink
	obs   metrics.Observer

	closed   atomic.Bool
	readOnly bool
	// dedup stores object content content-addressed and reference-counted.
	dedup bool

	// rotMu serializes rotation against normal operations.
	rotMu sync.RWMutex
}

// Info is the metadata reported by Stat and List.
type Info struct {
	// Key is the object key, relative to its bucket.
	Key string
	// Size is the object length in bytes.
	Size int64
	// ETag is the lowercase hex SHA-256 of the content.
	ETag string
	// ContentType and Metadata are caller-supplied metadata.
	ContentType string
	Metadata    map[string]string
	// CreatedAt is when the object record was first written.
	CreatedAt time.Time
}

// ObjectMeta is the user metadata carried by PutStream and SetMetadata.
type ObjectMeta struct {
	ContentType string
	Metadata    map[string]string
	CreatedAt   time.Time
}

// record is the persisted object. In dedup mode Data is empty and Size carries
// the content length; the bytes live under __blobs__ keyed by SHA256. A
// chunked record stores Size, ChunkSize and the ordered chunk hashes, with the
// bytes under __chunks__.
type record struct {
	SHA256      string            `json:"sha256"`
	Size        int64             `json:"size,omitempty"`
	Data        []byte            `json:"data,omitempty"`
	ContentType string            `json:"content_type,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at,omitempty"`
	ChunkSize   int               `json:"chunk_size,omitempty"`
	Chunks      []string          `json:"chunks,omitempty"`
}

// Open creates a Store over an already-open bbolt database. The caller owns db
// and must keep it open for the lifetime of the Store.
func Open(ctx context.Context, bdb *bolt.DB, cfg Config) (*Store, error) {
	return open(ctx, bdb, cfg, false)
}

// OpenReadOnly validates and reads an existing object store without writing. It
// is used by backup verification.
func OpenReadOnly(ctx context.Context, bdb *bolt.DB, cfg Config) (*Store, error) {
	return open(ctx, bdb, cfg, true)
}

func open(ctx context.Context, bdb *bolt.DB, cfg Config, readOnly bool) (*Store, error) {
	if ctx == nil {
		return nil, errors.New("object: nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if bdb == nil {
		return nil, ErrNoStore
	}
	for _, part := range cfg.Root {
		if err := validateBucket(string(part)); err != nil {
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
		trace:    cfg.Trace,
		obs:      cfg.Metrics,
		readOnly: readOnly,
		dedup:    cfg.Dedup,
	}
	s.enc.Store(enc)
	if err := s.ensureMeta(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func resolveEnc(cfg Config) (*encState, error) {
	if !cfg.EncryptValues && !cfg.Compress {
		return nil, nil
	}
	state := &encState{}
	if cfg.Compress {
		codec, err := compress.NewCodec(compress.Config{Level: cfg.CompressLevel, MinSize: cfg.MinCompress})
		if err != nil {
			return nil, err
		}
		state.comp = codec
	}
	if !cfg.EncryptValues {
		return state, nil
	}
	key, err := crypto.ResolveKey(cfg.Secret)
	if err != nil {
		return nil, err
	}
	kc, err := crypto.NewKeychain([][]byte{key})
	if err != nil {
		return nil, err
	}
	state.kc = kc
	return state, nil
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

// WithRoot returns a view rooted at the given bucket path.
func (s *Store) WithRoot(parts ...string) *Store {
	if s == nil {
		return nil
	}
	root := make([][]byte, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		if err := validateBucket(part); err != nil {
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
		trace:    s.trace,
		obs:      s.obs,
		readOnly: s.readOnly,
		dedup:    s.dedup,
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
		trace:    s.trace,
		obs:      s.obs,
		readOnly: s.readOnly,
		dedup:    s.dedup,
	}
}

// SetTraceSink replaces the sink that receives committed-write entries and
// returns the receiver. It is intended for a transaction-bound view.
func (s *Store) SetTraceSink(sink ledger.Sink) *Store {
	if s != nil {
		s.trace = sink
	}
	return s
}

func (s *Store) checkOpen() error {
	if s == nil || s.db == nil {
		return ErrNoStore
	}
	if s.rootStore().closed.Load() {
		return errors.New("object: store is closed")
	}
	return nil
}

// Close marks a standalone store closed. It never closes the shared handle.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.rootStore().closed.Store(true)
	return nil
}

// Flush is a no-op: object records are written synchronously.
func (s *Store) Flush() error { return s.checkOpen() }

func (s *Store) view(ctx context.Context, fn func(*bolt.Tx) error) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	root := s.rootStore()
	root.rotMu.RLock()
	defer root.rotMu.RUnlock()
	if s.tx != nil {
		return fn(s.tx)
	}
	return s.db.View(fn)
}

func (s *Store) update(ctx context.Context, fn func(*bolt.Tx) error) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if s.readOnly && s.tx == nil {
		return ErrReadOnly
	}
	root := s.rootStore()
	root.rotMu.RLock()
	defer root.rotMu.RUnlock()
	if s.tx != nil {
		return fn(s.tx)
	}
	return s.db.Update(fn)
}

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
		return nil, ErrInvalidBucket
	}
	return b, nil
}

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
		if v != FormatVersion && v != FormatVersionChunked {
			return fmt.Errorf("%w: object format %d, this build supports %d", ErrIncompatibleVersion, v, FormatVersionChunked)
		}
	}

	want := byte(0)
	enc := s.encState()
	if enc.encrypted() {
		want |= 1
	}
	if s.dedup {
		want |= modeDedup
	}
	mode := root.Get([]byte(modeKey))
	if mode == nil {
		if readOnly {
			return ErrWrongMode
		}
		if err := root.Put([]byte(modeKey), []byte{want}); err != nil {
			return err
		}
	} else if len(mode) != 1 {
		return fmt.Errorf("%w: malformed mode record (%d bytes)", ErrWrongMode, len(mode))
	} else if mode[0] != want {
		return fmt.Errorf("%w: stored mode %#x, configured %#x", ErrWrongMode, mode[0], want)
	}

	if !enc.encrypted() {
		return nil
	}
	canary := root.Get([]byte(canaryKey))
	if canary == nil {
		if readOnly {
			return ErrWrongMode
		}
		sealed, err := enc.kc.Seal([]byte(canaryPlaintext + "/0x01"))
		if err != nil {
			return err
		}
		return root.Put([]byte(canaryKey), sealed)
	}
	plain, err := enc.kc.Open(canary)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrWrongMode, err)
	}
	if string(plain) != canaryPlaintext+"/0x01" {
		return ErrWrongMode
	}
	return nil
}

// seal compresses plain when compression is enabled, then seals it when value
// encryption is enabled. The canary is sealed directly with the keychain and is
// never compressed.
func (s *Store) seal(plain []byte) ([]byte, error) {
	enc := s.encState()
	if enc != nil && enc.comp != nil {
		var err error
		if plain, err = enc.comp.Encode(nil, plain); err != nil {
			return nil, err
		}
	}
	if !enc.encrypted() {
		return plain, nil
	}
	return enc.kc.Seal(plain)
}

// openValue opens raw when value encryption is enabled and always decodes a
// compressed envelope.
func (s *Store) openValue(raw []byte) ([]byte, error) {
	enc := s.encState()
	if !enc.encrypted() {
		return compress.Decode(nil, raw)
	}
	plain, err := enc.kc.Open(raw)
	if err != nil {
		return nil, err
	}
	return compress.Decode(nil, plain)
}

func validateBucket(bucket string) error {
	if bucket == "" {
		return fmt.Errorf("%w: empty", ErrInvalidBucket)
	}
	if bucket != path.Clean(bucket) || strings.Contains(bucket, "/") || bucket == "." || bucket == ".." || strings.HasPrefix(bucket, "__") {
		return fmt.Errorf("%w: %q", ErrInvalidBucket, bucket)
	}
	return nil
}

func objectKey(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("%w: empty", ErrInvalidKey)
	}
	for _, seg := range strings.Split(key, "/") {
		if seg == ".." {
			return "", fmt.Errorf("%w: %q contains a parent segment", ErrInvalidKey, key)
		}
	}
	clean := strings.TrimPrefix(path.Clean("/"+key), "/")
	if clean == "" || clean == "." {
		return "", fmt.Errorf("%w: %q", ErrInvalidKey, key)
	}
	return clean, nil
}

func (s *Store) bucket(tx *bolt.Tx, name string, create bool) (*bolt.Bucket, error) {
	if err := validateBucket(name); err != nil {
		return nil, err
	}
	root := s.rootBucket(tx)
	if root == nil {
		if !create {
			return nil, nil
		}
		var err error
		root, err = s.createRootBucket(tx)
		if err != nil {
			return nil, err
		}
	}
	if create {
		return root.CreateBucketIfNotExists([]byte(name))
	}
	return root.Bucket([]byte(name)), nil
}

// Put writes r to bucket/key. The write is atomic: the reader is consumed
// completely before the record is written, so a failing reader leaves no object
// and leaves any previous content unchanged.
//
// size and contentType are accepted for compatibility and ignored.
func (s *Store) Put(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string) (string, error) {
	if err := ctxErr(ctx); err != nil {
		return "", err
	}
	if s == nil || s.db == nil {
		return "", ErrNoStore
	}
	okey, err := objectKey(key)
	if err != nil {
		return "", err
	}
	if err := validateBucket(bucket); err != nil {
		return "", err
	}
	if r == nil {
		return "", fmt.Errorf("object: put %q: reader is required", key)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("object: put %q: %w", key, err)
	}
	sum := sha256.Sum256(data)
	hashHex := hex.EncodeToString(sum[:])
	rec := record{
		SHA256:      hashHex,
		ContentType: contentType,
		CreatedAt:   time.Now().UTC(),
	}
	if s.dedupEnabled() {
		rec.Size = int64(len(data))
	} else {
		rec.Data = data
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return "", err
	}
	sealed, err := s.seal(raw)
	if err != nil {
		return "", err
	}
	var entries []ledger.Entry
	err = s.update(ctx, func(tx *bolt.Tx) error {
		b, err := s.bucket(tx, bucket, true)
		if err != nil {
			return err
		}
		if old := b.Get([]byte(okey)); old != nil {
			var previous record
			if err := s.recordFromRaw(old, &previous); err != nil {
				return err
			}
			if err := s.releaseRecord(tx, &previous); err != nil {
				return err
			}
		}
		if s.dedupEnabled() {
			if err := s.acquireBlob(tx, hashHex, data); err != nil {
				return err
			}
		}
		if err := b.Put([]byte(okey), sealed); err != nil {
			return err
		}
		if s.trace != nil {
			entries = append(entries, ledger.Entry{Store: ledger.StoreObject, Op: "put", Entity: bucket + ":" + okey, Digest: sum[:]})
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("object: put %q: %w", key, err)
	}
	return "", s.replay(entries)
}

// Get opens bucket/key for reading. The caller must Close the reader.
func (s *Store) Get(ctx context.Context, bucket, key string) (io.ReadCloser, int64, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, 0, err
	}
	if s == nil || s.db == nil {
		return nil, 0, ErrNoStore
	}
	okey, err := objectKey(key)
	if err != nil {
		return nil, 0, err
	}
	var (
		rec  record
		data []byte
	)
	err = s.view(ctx, func(tx *bolt.Tx) error {
		b, err := s.bucket(tx, bucket, false)
		if err != nil {
			return err
		}
		if b == nil {
			return os.ErrNotExist
		}
		if err := s.getRecord(b, okey, &rec); err != nil {
			return err
		}
		data, err = s.recordData(tx, &rec)
		return err
	})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, fmt.Errorf("object: get %q: %w", key, os.ErrNotExist)
		}
		return nil, 0, fmt.Errorf("object: get %q: %w", key, err)
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}

// ReadAll reads a whole object.
func (s *Store) ReadAll(ctx context.Context, bucket, key string) ([]byte, error) {
	rc, _, err := s.Get(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// Delete removes bucket/key. A missing object is not an error.
func (s *Store) Delete(ctx context.Context, bucket, key string) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return ErrNoStore
	}
	okey, err := objectKey(key)
	if err != nil {
		return err
	}
	var entries []ledger.Entry
	err = s.update(ctx, func(tx *bolt.Tx) error {
		b, err := s.bucket(tx, bucket, false)
		if err != nil || b == nil {
			return err
		}
		raw := b.Get([]byte(okey))
		if raw == nil {
			return nil
		}
		var rec record
		if err := s.recordFromRaw(raw, &rec); err != nil {
			return err
		}
		if err := b.Delete([]byte(okey)); err != nil {
			return err
		}
		if err := s.releaseRecord(tx, &rec); err != nil {
			return err
		}
		if s.trace != nil {
			entries = append(entries, ledger.Entry{Store: ledger.StoreObject, Op: "delete", Entity: bucket + ":" + okey})
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("object: delete %q: %w", key, err)
	}
	return s.replay(entries)
}

// Move relocates one object from srcKey to dstKey inside bucket. A missing
// source is a no-op, matching the previous blob semantics.
func (s *Store) Move(ctx context.Context, bucket, srcKey, dstKey string) error {
	if srcKey == "" || dstKey == "" || srcKey == dstKey {
		return nil
	}
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return ErrNoStore
	}
	src, err := objectKey(srcKey)
	if err != nil {
		return err
	}
	dst, err := objectKey(dstKey)
	if err != nil {
		return err
	}
	var entries []ledger.Entry
	err = s.update(ctx, func(tx *bolt.Tx) error {
		b, err := s.bucket(tx, bucket, false)
		if err != nil || b == nil {
			return err
		}
		raw := b.Get([]byte(src))
		if raw == nil {
			return nil
		}
		if err := b.Delete([]byte(src)); err != nil {
			return err
		}
		if err := b.Put([]byte(dst), raw); err != nil {
			return err
		}
		if s.trace != nil {
			entries = append(entries, ledger.Entry{Store: ledger.StoreObject, Op: "move", Entity: bucket + ":" + src + "->" + dst})
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("object: move %q -> %q: %w", srcKey, dstKey, err)
	}
	return s.replay(entries)
}

// Stat reports one object's metadata. A missing object is not an error.
func (s *Store) Stat(ctx context.Context, bucket, key string) (Info, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return Info{}, false, err
	}
	if s == nil || s.db == nil {
		return Info{}, false, ErrNoStore
	}
	okey, err := objectKey(key)
	if err != nil {
		return Info{}, false, err
	}
	var rec record
	err = s.view(ctx, func(tx *bolt.Tx) error {
		b, err := s.bucket(tx, bucket, false)
		if err != nil {
			return err
		}
		if b == nil {
			return os.ErrNotExist
		}
		return s.getRecord(b, okey, &rec)
	})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Info{}, false, nil
		}
		return Info{}, false, fmt.Errorf("object: stat %q: %w", key, err)
	}
	return s.infoOf(okey, rec), true, nil
}

// Exists reports whether bucket/key holds bytes.
func (s *Store) Exists(ctx context.Context, bucket, key string) bool {
	_, ok, err := s.Stat(ctx, bucket, key)
	return err == nil && ok
}

// ListOptions filters ListWithOptions.
type ListOptions struct {
	Prefix   string
	Metadata map[string]string
}

// List returns every object under bucket whose key begins with prefix,
// recursively, sorted by key. An absent bucket or prefix is empty.
func (s *Store) List(ctx context.Context, bucket, prefix string) ([]Info, error) {
	return s.ListWithOptions(ctx, bucket, ListOptions{Prefix: prefix})
}

// ListWithOptions is List with a metadata filter. Every supplied metadata pair
// must match exactly.
func (s *Store) ListWithOptions(ctx context.Context, bucket string, opts ListOptions) ([]Info, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	if s == nil || s.db == nil {
		return nil, ErrNoStore
	}
	cleanPrefix := ""
	var err error
	if opts.Prefix != "" {
		cleanPrefix, err = objectKey(opts.Prefix)
		if err != nil {
			return nil, err
		}
	}
	var out []Info
	err = s.view(ctx, func(tx *bolt.Tx) error {
		b, err := s.bucket(tx, bucket, false)
		if err != nil {
			return err
		}
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			if v == nil {
				return nil
			}
			key := string(k)
			if cleanPrefix != "" && !strings.HasPrefix(key, cleanPrefix) {
				return nil
			}
			var rec record
			if err := s.recordFromRaw(v, &rec); err != nil {
				return err
			}
			if !metadataMatches(rec.Metadata, opts.Metadata) {
				return nil
			}
			out = append(out, s.infoOf(key, rec))
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("object: list %q: %w", opts.Prefix, err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func metadataMatches(have, want map[string]string) bool {
	for key, value := range want {
		if have[key] != value {
			return false
		}
	}
	return true
}

func (s *Store) getRecord(b *bolt.Bucket, key string, dest *record) error {
	raw := b.Get([]byte(key))
	if raw == nil {
		return os.ErrNotExist
	}
	return s.recordFromRaw(raw, dest)
}

func (s *Store) recordFromRaw(raw []byte, dest *record) error {
	plain, err := s.openValue(raw)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(plain, dest); err != nil {
		return fmt.Errorf("object: decode record: %w", err)
	}
	return nil
}

func (s *Store) replay(entries []ledger.Entry) error {
	if s.trace == nil {
		return nil
	}
	for _, entry := range entries {
		if err := s.trace.Record(entry); err != nil {
			return err
		}
	}
	return nil
}

// MigrateFromFile copies every object in a file-backed blob bucket into this
// store. The fstree store continues to hold the originals; nothing is deleted.
// It is the explicit migration path for data written by the previous blob
// implementation.
func (s *Store) MigrateFromFile(ctx context.Context, fsys *fstree.FS, bucket string) error {
	if s == nil || s.db == nil {
		return ErrNoStore
	}
	if fsys == nil {
		return errors.New("object: migrate: file system is required")
	}
	if err := validateBucket(bucket); err != nil {
		return err
	}
	root := path.Join("/", bucket)
	if _, err := fsys.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("object: migrate %q: %w", bucket, err)
	}
	return fstree.Walk(fsys, root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		data, err := fsys.ReadFile(p)
		if err != nil {
			return err
		}
		key := strings.TrimPrefix(p, root+"/")
		_, err = s.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "")
		return err
	})
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return errors.New("object: nil context")
	}
	return ctx.Err()
}

// ── grants ───────────────────────────────────────────────────────────────────

// GrantType identifies how a bearer reaches the object.
type GrantType string

const (
	// GrantLocal means the owning application serves the bytes itself; the bearer
	// uses the application's own data plane and its own session.
	GrantLocal GrantType = "local"
)

// Grant is a short-lived authorization to touch ONE object.
//
// It is deliberately not a credential: it records which object was authorized,
// for which method, and until when. Deciding whether a caller MAY be granted is
// the application's job — that is where the access-control data lives — so this
// type only carries the bookkeeping.
type Grant struct {
	Type      GrantType `json:"type"`
	Method    string    `json:"method"` // "GET" or "PUT"
	Bucket    string    `json:"bucket"`
	Key       string    `json:"key"`
	ExpiresAt time.Time `json:"expires_at"`
}

// DefaultGrantTTL is used when a Minter is built with a non-positive TTL.
const DefaultGrantTTL = 15 * time.Minute

// Minter stamps grants with an expiry.
type Minter struct {
	ttl time.Duration
	now func() time.Time
}

// NewMinter returns a Minter with the given TTL. ttl <= 0 uses DefaultGrantTTL.
func NewMinter(ttl time.Duration) *Minter {
	if ttl <= 0 {
		ttl = DefaultGrantTTL
	}
	return &Minter{ttl: ttl, now: time.Now}
}

// Mint returns a grant for one object and method. ttl <= 0 uses the minter's TTL.
func (m *Minter) Mint(bucket, key, method string, ttl time.Duration) Grant {
	if ttl <= 0 {
		ttl = m.ttl
	}
	now := time.Now
	if m.now != nil {
		now = m.now
	}
	return Grant{
		Type:      GrantLocal,
		Method:    method,
		Bucket:    bucket,
		Key:       key,
		ExpiresAt: now().Add(ttl),
	}
}

// TTL reports the minter's default grant lifetime.
func (m *Minter) TTL() time.Duration { return m.ttl }

// Buckets lists the object bucket names under the store's root, sorted. The
// store's own metadata records live as plain keys under the root, so only real
// child buckets (and never a name starting with "__") are reported.
func (s *Store) Buckets(ctx context.Context) ([]string, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	if s == nil || s.db == nil {
		return nil, ErrNoStore
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
		return nil, fmt.Errorf("object: list buckets: %w", err)
	}
	sort.Strings(names)
	return names, nil
}

// DropBucket removes an entire object bucket and every key in it. A missing
// bucket is a no-op. One "drop_bucket" entry is traced when tracing is
// enabled.
func (s *Store) DropBucket(ctx context.Context, bucket string) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return ErrNoStore
	}
	if err := validateBucket(bucket); err != nil {
		return err
	}
	var entries []ledger.Entry
	err := s.update(ctx, func(tx *bolt.Tx) error {
		root := s.rootBucket(tx)
		if root == nil || root.Bucket([]byte(bucket)) == nil {
			return nil
		}
		b := root.Bucket([]byte(bucket))
		if err := b.ForEach(func(k, v []byte) error {
			if v == nil {
				return nil
			}
			var rec record
			if err := s.recordFromRaw(v, &rec); err != nil {
				return err
			}
			return s.releaseRecord(tx, &rec)
		}); err != nil {
			return err
		}
		if err := root.DeleteBucket([]byte(bucket)); err != nil {
			return err
		}
		if s.trace != nil {
			entries = append(entries, ledger.Entry{Store: ledger.StoreObject, Op: "drop_bucket", Entity: bucket})
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("object: drop bucket %q: %w", bucket, err)
	}
	return s.replay(entries)
}
