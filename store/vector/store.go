package vector

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/malivvan/harbor/pkg/audit"
	"github.com/malivvan/harbor/pkg/metrics"
	"go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
)

// Name is the conventional root bucket the harbor root package uses for the
// vec sub-store. It is not applied implicitly: an empty Config.Root means the
// database root.
const Name = "vec"

// ErrClosed is returned when an operation targets a closed store.
var ErrClosed = errors.New("vec: storage is closed")

const collectionsMetaBucket = "__collections_meta__"

// reservedTopLevelBuckets are the top-level buckets Harbor itself creates in a
// shared database. They are never vector collections, even when a vec Store is
// opened at the database root.
var reservedTopLevelBuckets = map[string]bool{
	collectionsMetaBucket: true,
	"db":                  true,
	"fs":                  true,
	"ts":                  true,
	"vec":                 true,
	"harbor.Metadata":     true,
}

// Store handles local persistence using BoltDB (bbolt).
// It provides durable storage for vector collections and their points.
type Store struct {
	db *bbolt.DB
	// tx, when non-nil, binds every operation to a caller-owned transaction.
	// See WithTransaction.
	tx *bbolt.Tx
	// base points at the store this value is a rooted view of, or nil for a
	// store returned by Open. Views share the handle, encryption state, pools
	// and flush registry with their base.
	base *Store
	// views is set on the base and maps a root key to the view for that root.
	views map[string]*Store
	// dropped records collections dropped inside a bound transaction so the
	// live store forgets them when the transaction commits.
	dropped map[string]struct{}
	// readOnly marks a view bound to a read-only transaction; constructors
	// then open existing collections without writing metadata.
	readOnly bool

	// root is the parent bucket path for collections and collection metadata.
	// The default is the "vec" bucket.
	root [][]byte

	// closed reports whether Close has been called. It is an atomic so that
	// Close can race safely with the other Store methods.
	closed    atomic.Bool
	ownsDB    atomic.Bool
	quantizer Quantizer
	useQuant  bool

	// mu guards collections. closeMu serializes Close. flushMu serializes
	// Flush/Close against new buffered writes.
	mu          sync.Mutex
	closeMu     sync.Mutex
	flushMu     sync.Mutex
	collections map[string]*Collection

	// enc is the optional encryption state. It is an atomic pointer so a
	// rotation can swap the keys without racing readers.
	enc atomic.Pointer[encState]

	// rotMu serializes rotation against normal operations. Normal view/update
	// transactions take RLock; rotation takes Lock across the rewrite and the
	// adoption of the new key state.
	rotMu sync.RWMutex

	// audit receives an entry for every committed write, or nil when the store
	// was opened without audit. It is set once at Open and never mutated.
	audit audit.Sink

	// obs receives operation metrics and spans, or nil when the store was
	// opened without instrumentation. It is set once at Open and never
	// mutated.
	obs metrics.Observer
}

// auditEntry records one committed write in the store's audit trail. It is a
// no-op when the store has no audit sink. Digest is the SHA-256 of the written
// payload, or nil for deletes.
func (s *Store) auditEntry(op, collection string, digest []byte) error {
	if s == nil || s.audit == nil {
		return nil
	}
	return s.audit.Record(audit.Entry{
		Store:  audit.StoreVec,
		Op:     op,
		Entity: collection,
		Digest: digest,
	})
}

// digestOf returns the SHA-256 of data, or nil when data is empty.
func digestOf(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	sum := sha256.Sum256(data)
	return sum[:]
}

// resultLabel maps an error to the ResultOK/ResultError label value.
func resultLabel(err error) string {
	if err != nil {
		return metrics.ResultError
	}
	return metrics.ResultOK
}

// view runs a read-only transaction while excluding rotation. A store bound to
// an explicit transaction reads from that transaction instead.
func (s *Store) view(fn func(tx *bbolt.Tx) error) error {
	if s.tx != nil {
		return fn(s.tx)
	}
	mu := s.rotMuOf()
	mu.RLock()
	defer mu.RUnlock()
	return s.db.View(fn)
}

// update runs a write transaction while excluding rotation. A store bound to an
// explicit transaction writes into that transaction instead and never commits.
func (s *Store) update(fn func(tx *bbolt.Tx) error) error {
	if s.tx != nil {
		return fn(s.tx)
	}
	mu := s.rotMuOf()
	mu.RLock()
	defer mu.RUnlock()
	return s.db.Update(fn)
}

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

// WithRoot returns a view of the store rooted at the given bucket path. The
// view shares the handle, encryption state, quantization settings and flush
// registry with the receiver; collections opened through it are kept apart.
// Repeated calls with the same path return the same view. No segments means
// the database root; pass the conventional root explicitly (for example "vec")
// to keep it.
func (s *Store) WithRoot(parts ...string) *Store {
	if s == nil {
		return nil
	}
	owner := s.rootStore()
	root := make([][]byte, 0, len(parts))
	keyParts := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		root = append(root, []byte(part))
		keyParts = append(keyParts, part)
	}
	key := strings.Join(keyParts, "\x00")

	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.views == nil {
		owner.views = make(map[string]*Store)
	}
	if existing := owner.views[key]; existing != nil {
		return existing
	}
	view := &Store{
		db:          owner.db,
		tx:          s.tx,
		base:        owner,
		root:        root,
		readOnly:    s.readOnly,
		collections: make(map[string]*Collection),
		dropped:     make(map[string]struct{}),
		quantizer:   owner.quantizer,
		useQuant:    owner.useQuant,
		audit:       owner.audit,
		obs:         owner.obs,
	}
	owner.views[key] = view
	return view
}

// SetAuditSink replaces the sink that receives committed-write events and
// returns the receiver. It is intended for a transaction-bound view created by
// WithTransaction: the view stages events and the transaction owner records
// them after it commits. It must not be called concurrently with writes on the
// same Store.
func (s *Store) SetAuditSink(sink audit.Sink) *Store {
	if s != nil {
		s.audit = sink
	}
	return s
}

// WithTransaction returns a Store bound to tx. Every read and write goes
// through tx; the bound store owns neither the transaction nor its handle.
// Collections opened through the bound view live only in its in-memory map, so
// a search inside the transaction sees staged points and the committed state.
//
// Commit a cross-store transaction in three steps: FlushStaged, commit tx, then
// AdoptTransaction(view).
func (s *Store) WithTransaction(tx *bbolt.Tx) *Store {
	if s == nil || tx == nil {
		return nil
	}
	bound := &Store{
		db:          s.db,
		tx:          tx,
		base:        s.rootStore(),
		readOnly:    !tx.Writable(),
		root:        cloneRoot(s.root),
		quantizer:   s.quantizer,
		useQuant:    s.useQuant,
		collections: make(map[string]*Collection),
		dropped:     make(map[string]struct{}),
		audit:       s.audit,
		obs:         s.obs,
	}
	bound.ownsDB.Store(false)
	return bound
}

// FlushStaged writes every buffered point of a transaction-bound view into its
// transaction. The caller must call it before committing the transaction so
// staged points are part of it.
func (s *Store) FlushStaged() error {
	if s == nil || s.tx == nil {
		return ErrInvalidConfig
	}
	return s.flushCollections()
}

// AdoptTransaction installs the collection state built by a committed
// transaction view into s, rebinding the collections to the live store. It must
// be called after the caller committed the transaction; on abort it is not
// called and the view is discarded.
//
// The transaction view's indexes were loaded from the committed database and
// then updated with the staged writes, so adopting them avoids re-reading every
// point. Collections dropped inside the transaction are removed from s.
func (s *Store) AdoptTransaction(view *Store) {
	if s == nil || view == nil {
		return
	}
	view.mu.Lock()
	adopted := make(map[string]*Collection, len(view.collections))
	for name, c := range view.collections {
		adopted[name] = c
	}
	dropped := make([]string, 0, len(view.dropped))
	for name := range view.dropped {
		dropped = append(dropped, name)
	}
	view.mu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.collections == nil {
		s.collections = make(map[string]*Collection)
	}
	for _, name := range dropped {
		delete(s.collections, name)
	}
	for name, c := range adopted {
		c.mu.Lock()
		c.storage = s
		c.writeBuf = nil
		c.mu.Unlock()
		s.collections[name] = c
	}
}

// payloadJSONKey is the reserved stored-payload key that carries one JSON
// document for every complex payload value of a point. It keeps the wire
// format backward compatible: records written by older versions simply have
// per-key KindJSON values instead.
const payloadJSONKey = "\x00__harbor_json_payload__"

func toStoredPoint(p *PointStruct) (*StoredPoint, error) {
	stored := &StoredPoint{
		ID:      p.ID,
		Vector:  p.Vector,
		Version: p.Version,
	}

	if p.Payload != nil {
		stored.Payload = make(map[string]*Value)
		complexValues := make(map[string]any)
		for k, v := range p.Payload {
			value := &Value{}
			switch val := v.(type) {
			case string:
				if !utf8.ValidString(val) {
					val = string(bytes.ToValidUTF8([]byte(val), nil)) // sanitize: replace invalid UTF-8 sequences
				}
				value.Kind = KindString
				value.StringValue = val
			case int:
				value.Kind = KindInt
				value.IntValue = int64(val)
			case int64:
				value.Kind = KindInt
				value.IntValue = val
			case float32:
				value.Kind = KindDouble
				value.DoubleValue = float64(val)
			case float64:
				value.Kind = KindDouble
				value.DoubleValue = val
			case bool:
				value.Kind = KindBool
				value.BoolValue = val
			case []byte:
				value.Kind = KindBytes
				value.BytesValue = val
			default:
				// Complex values (maps, slices, ...) are collected and encoded
				// once into a single JSON document below.
				complexValues[k] = val
				continue
			}
			stored.Payload[k] = value
		}

		if len(complexValues) > 0 {
			jsonBytes, err := json.Marshal(complexValues)
			if err != nil {
				// Report which key failed, matching the old per-value errors.
				for k := range complexValues {
					if _, keyErr := json.Marshal(p.Payload[k]); keyErr != nil {
						return nil, fmt.Errorf("payload key %q: %w", k, keyErr)
					}
				}
				return nil, err
			}
			stored.Payload[payloadJSONKey] = &Value{
				Kind:       KindJSON,
				BytesValue: jsonBytes,
			}
		}
	}

	return stored, nil
}

func fromStoredPoint(stored *StoredPoint) *PointStruct {
	p := &PointStruct{
		ID:      stored.ID,
		Vector:  stored.Vector,
		Version: stored.Version,
	}

	if stored.Payload != nil {
		p.Payload = make(Payload, len(stored.Payload))

		// One JSON document holds every complex value of this point.
		if blob, ok := stored.Payload[payloadJSONKey]; ok && blob != nil && blob.Kind == KindJSON {
			if decoded, ok := decodeJSONPayload(blob.BytesValue).(map[string]any); ok {
				for k, v := range decoded {
					p.Payload[k] = v
				}
			}
		}

		for k, v := range stored.Payload {
			if k == payloadJSONKey {
				continue
			}
			if v == nil {
				continue
			}
			switch v.Kind {
			case KindString:
				p.Payload[k] = v.StringValue
			case KindInt:
				p.Payload[k] = v.IntValue
			case KindDouble:
				p.Payload[k] = v.DoubleValue
			case KindBool:
				p.Payload[k] = v.BoolValue
			case KindBytes:
				// Raw bytes round-trip as raw bytes: never reinterpret them as
				// JSON, which used to change their type.
				p.Payload[k] = v.BytesValue
			case KindJSON:
				// Complex values are JSON documents. Numbers are decoded with
				// json.Number and normalized so that integers stay integers.
				p.Payload[k] = decodeJSONPayload(v.BytesValue)
			}
		}
	}

	return p
}

// decodeJSONPayload decodes a JSON payload document, preserving integer types
// instead of turning every number into a float64. Invalid documents are
// returned as raw bytes.
func decodeJSONPayload(data []byte) any {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	var obj any
	if err := dec.Decode(&obj); err != nil {
		return data
	}
	return normalizeJSONValue(obj)
}

// normalizeJSONValue converts json.Number values into int64 when they are
// integral, and float64 otherwise, recursively.
func normalizeJSONValue(v any) any {
	switch n := v.(type) {
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i
		}
		if f, err := n.Float64(); err == nil {
			return f
		}
		return n.String()
	case map[string]any:
		for k, item := range n {
			n[k] = normalizeJSONValue(item)
		}
		return n
	case []any:
		for i, item := range n {
			n[i] = normalizeJSONValue(item)
		}
		return n
	default:
		return v
	}
}

// Open initializes a new BoltDB storage engine with optional vector quantization
// and native AEAD crypto.
//
// A nil database is rejected with ErrInvalidConfig: the returned Store would
// have no way to persist anything and every method would otherwise panic.
// cfg.Root is an optional bucket path used as the parent for all collections
// and collection metadata; an empty path means the database root.
// Returns an error if the context is invalid.
//
// A zero cfg creates a plaintext store with no quantization and its on-disk
// format is unchanged. cfg.UseQuant enables quantization; when cfg.Quantizer is
// nil the default SQ8 quantizer is used. cfg.Secret and the Encrypt fields
// enable optional crypto.
//
// When encryption is enabled the store writes an unencrypted marker into the
// collections metadata bucket and verifies it with a sealed canary, so:
//
//   - opening an encrypted store without a Config (or with the wrong settings)
//     fails with ErrWrongMode,
//   - opening an encrypted store with the wrong key fails with ErrAuthFailed,
//   - opening a store that already holds plaintext collections with encryption
//     requested fails with ErrWrongMode.
//
// Migrating an existing plaintext store to an encrypted one is not supported;
// re-import the data instead.
func Open(ctx context.Context, bdb *bbolt.DB, cfg Config) (*Store, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	cfg, err := cfg.Normalize()
	if err != nil {
		return nil, err
	}

	enc, err := resolveConfig(cfg)
	if err != nil {
		return nil, err
	}
	if bdb == nil {
		// A nil database cannot store anything: every method would panic. Fail
		// fast instead of handing out a handle that explodes on first use.
		return nil, fmt.Errorf("%w: database handle is nil", ErrInvalidConfig)
	}

	store := &Store{
		db:          bdb,
		quantizer:   cfg.Quantizer,
		useQuant:    cfg.UseQuant,
		root:        cfg.Root,
		collections: make(map[string]*Collection),
		audit:       cfg.Audit,
		obs:         cfg.Metrics,
	}
	store.enc.Store(enc)
	// Standalone use owns the handle; an embedding store sets SharedDB so
	// Close never closes a handle it did not open.
	store.ownsDB.Store(!cfg.SharedDB)

	if err := store.checkMode(ctx); err != nil {
		return nil, err
	}

	return store, nil
}

// ErrStorageNotOpen is returned when a Store method is called on a store that
// has no underlying database (for example a zero-value Store).
var ErrStorageNotOpen = errors.New("storage is not open")

// checkOpen reports whether the store can serve requests. Every exported
// method calls it before touching the database so that a Store without a
// database returns an error instead of panicking. A nil or zero-value Store is
// reported as closed or not open, never as a panic.
func (s *Store) checkOpen() error {
	if s == nil {
		return ErrClosed
	}
	if s.rootStore().closed.Load() || s.closed.Load() {
		return ErrClosed
	}
	if s.db == nil {
		return ErrStorageNotOpen
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

	m, canary, found, err := s.loadMarker()
	if err != nil {
		return err
	}

	enc := s.encState()
	if enc == nil {
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

	// No marker: only a store without data may be initialized as encrypted.
	hasData, err := s.hasData()
	if err != nil {
		return err
	}
	if hasData {
		return fmt.Errorf("%w: store already contains plaintext data", ErrWrongMode)
	}

	return s.storeMarker(enc)
}

// ErrNilContext is returned when a nil context is passed to an operation.
var ErrNilContext = fmt.Errorf("context is nil")

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	return ctx.Err()
}

// rootBucket traverses the configured root path, or returns nil when the path
// is empty or any element is missing.
func (s *Store) rootBucket(tx *bbolt.Tx) *bbolt.Bucket {
	var b *bbolt.Bucket
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

// createRootBucket creates the configured root path and returns its innermost
// bucket. An empty path returns nil.
func (s *Store) createRootBucket(tx *bbolt.Tx) (*bbolt.Bucket, error) {
	var b *bbolt.Bucket
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

// collectionBucket returns the bucket for a collection under the configured
// root, or nil when either the root or collection bucket does not exist.
func (s *Store) collectionBucket(tx *bbolt.Tx, name string) (*bbolt.Bucket, error) {
	key, err := s.bucketKey(name)
	if err != nil {
		return nil, err
	}

	if len(s.root) == 0 {
		return tx.Bucket(key), nil
	}

	root := s.rootBucket(tx)
	if root == nil {
		return nil, nil
	}
	return root.Bucket(key), nil
}

// createCollectionBucket creates the collection bucket under the configured
// root, creating the root bucket when necessary.
func (s *Store) createCollectionBucket(tx *bbolt.Tx, name string) (*bbolt.Bucket, error) {
	key, err := s.bucketKey(name)
	if err != nil {
		return nil, fmt.Errorf("failed to derive the bucket key for collection %q: %w", name, err)
	}

	if len(s.root) == 0 {
		return tx.CreateBucketIfNotExists(key)
	}

	root, err := s.createRootBucket(tx)
	if err != nil {
		return nil, err
	}
	if root == nil {
		return nil, fmt.Errorf("%w: root path is empty", ErrInvalidConfig)
	}
	return root.CreateBucketIfNotExists(key)
}

// metaBucket returns the collection metadata bucket under the configured root.
// When create is true, missing buckets are created.
func (s *Store) metaBucket(tx *bbolt.Tx, create bool) (*bbolt.Bucket, error) {
	var parent *bbolt.Bucket
	var err error

	if len(s.root) > 0 {
		if create {
			parent, err = s.createRootBucket(tx)
		} else {
			parent = s.rootBucket(tx)
		}
		if err != nil {
			return nil, err
		}
		if parent == nil {
			if create {
				return nil, fmt.Errorf("%w: root path is empty", ErrInvalidConfig)
			}
			return nil, nil
		}
		if create {
			return parent.CreateBucketIfNotExists([]byte(collectionsMetaBucket))
		}
		return parent.Bucket([]byte(collectionsMetaBucket)), nil
	}

	if create {
		return tx.CreateBucketIfNotExists([]byte(collectionsMetaBucket))
	}
	return tx.Bucket([]byte(collectionsMetaBucket)), nil
}

// Close flushes the buffered writes of every collection this Store created and
// then closes the database handle.
//
// The caller owns the bbolt handle passed to Open; Store.Close closes it as a
// convenience for standalone use. An embedding store that shares one handle
// (the harbor root package) must call Flush instead of Close and close the
// handle itself. Close is idempotent.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.closeMu.Lock()
	defer s.closeMu.Unlock()

	if s.closed.Load() {
		return nil
	}
	if err := s.flushCollections(); err != nil {
		return err
	}
	s.closed.Store(true)

	if s.db == nil || !s.ownsDB.Load() {
		return nil
	}

	if err := s.db.Close(); err != nil {
		// The database may still be usable: allow a retry.
		s.closed.Store(false)
		return err
	}
	return nil
}

// SetOwnsDB controls whether Close closes the bbolt handle. An embedding store
// that shares one handle (the harbor root package) sets it to false; standalone
// users keep the default true. Config.SharedDB sets the same flag at Open time.
func (s *Store) SetOwnsDB(owns bool) {
	if s != nil {
		s.ownsDB.Store(owns)
	}
}

// Closed reports whether Close has finished. A closed store rejects every
// operation with ErrClosed. A nil store is reported as closed.
func (s *Store) Closed() bool {
	return s == nil || s.closed.Load()
}

// Flush writes the buffered points of every collection created by this Store
// without closing the database. Embedders that own the bbolt handle use it
// before closing the handle. A closed collection is skipped: Close already
// flushed it.
func (s *Store) Flush() error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	return s.flushCollections()
}

// trackCollection remembers a collection so Close/Flush can drain its buffer.
func (s *Store) trackCollection(c *Collection) {
	if s == nil || c == nil {
		return
	}
	s.mu.Lock()
	if s.collections == nil {
		s.collections = make(map[string]*Collection)
	}
	s.collections[c.Name] = c
	s.mu.Unlock()
}

// untrackCollection forgets a collection whose Close already flushed it, so
// Store.Flush and Store.Close no longer visit it.
func (s *Store) untrackCollection(name string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	delete(s.collections, name)
	s.mu.Unlock()
}

// flushCollections drains the write buffer of every tracked collection.
func (s *Store) flushCollections() error {
	// No store-wide flush lock here: each Collection.Flush serializes its own
	// commit on the shared flushMu. Taking it here as well would self-deadlock.
	owner := s.rootStore()
	collections := make([]*Collection, 0)
	gather := func(store *Store) {
		store.mu.Lock()
		for _, c := range store.collections {
			collections = append(collections, c)
		}
		store.mu.Unlock()
	}
	gather(s)
	if s == owner {
		owner.mu.Lock()
		views := make([]*Store, 0, len(owner.views))
		for _, view := range owner.views {
			views = append(views, view)
		}
		owner.mu.Unlock()
		for _, view := range views {
			gather(view)
		}
	}

	for _, c := range collections {
		if c.Closed() {
			continue
		}
		if err := c.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// EnsureCollection creates a bucket for the collection if it doesn't already exist.
// Each collection is stored as a separate bucket in BoltDB.
func (s *Store) EnsureCollection(colName string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	if err := s.update(func(tx *bbolt.Tx) error {
		_, err := s.createCollectionBucket(tx, colName)
		return err
	}); err != nil {
		return err
	}
	return s.auditEntry("create_collection", colName, nil)
}

// UpsertPoints saves or updates a batch of points to disk. Points are encoded
// with the positional binary format described in binary.go and stored with
// their ID as the key. If quantization is enabled, vectors are compressed
// before storage. Returns an error if the collection does not exist or if
// serialization fails.
func (s *Store) UpsertPoints(colName string, points []PointStruct) error {
	if err := validatePayloadKeys(points); err != nil {
		return err
	}
	if err := s.checkOpen(); err != nil {
		return err
	}

	if err := s.update(func(tx *bbolt.Tx) error {
		b, err := s.collectionBucket(tx, colName)
		if err != nil {
			return err
		}
		if b == nil {
			return fmt.Errorf("%w: %s", ErrCollectionNotFound, colName)
		}
		// Convert and size every point first, then encode the batch into one
		// exact-size arena. bbolt keeps the value slices until the transaction
		// commits, so one shared arena is safe where a reused scratch buffer
		// would overwrite earlier values.
		storedPoints := make([]*StoredPoint, len(points))
		total := 0
		for i := range points {
			p := &points[i]

			// Quantize vector if enabled
			if s.useQuant && s.quantizer != nil {
				// Create a copy to avoid modifying the original
				quantizedPoint := *p
				quantizedPoint.Vector = nil // Clear vector for storage

				// Store the quantized vector in a private copy of the payload:
				// mutating the caller's map (or the in-memory index's map)
				// would expose the reserved key in public results.
				// A shallow copy is enough: only the reserved key is added,
				// and payload values are treated as immutable.
				quantizedPoint.Payload = make(Payload, len(p.Payload)+1)
				for k, v := range p.Payload {
					quantizedPoint.Payload[k] = v
				}
				quantizedPoint.Payload["__quantized_vector"] = s.quantizer.Quantize(p.Vector)

				p = &quantizedPoint
			}

			stored, err := toStoredPoint(p)
			if err != nil {
				return fmt.Errorf("failed to encode point %s: %w", p.ID, err)
			}
			storedPoints[i] = stored
			total += storedPointSize(*stored)
		}

		arena := make([]byte, 0, total)
		for i, stored := range storedPoints {
			p := &points[i]
			start := len(arena)
			arena = appendStoredPoint(arena, *stored)
			sealed, err := s.sealBytes(arena[start:])
			if err != nil {
				return fmt.Errorf("failed to encrypt point %s: %w", p.ID, err)
			}

			key, err := s.keyBytes(p.ID)
			if err != nil {
				return fmt.Errorf("failed to derive the key for point %q: %w", p.ID, err)
			}

			if err := b.Put(key, sealed); err != nil {
				return fmt.Errorf("failed to write point %s to bbolt: %w", p.ID, err)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return s.auditEntry("upsert", colName, digestOf(encodePointsDigest(points)))
}

// LoadCollection loads all points for a collection from disk into memory.
// Returns a map of point IDs to PointStruct pointers.
// If quantization is enabled, compressed vectors are decompressed during loading.
// If the collection doesn't exist, returns an empty map without error.
func (s *Store) LoadCollection(colName string) (map[string]*PointStruct, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}

	// The quantization mode is persisted in the collection metadata; loading a
	// quantized collection without its quantizer would silently return nil
	// vectors.
	meta, metaErr := s.LoadCollectionMeta(colName)
	if metaErr != nil && !errors.Is(metaErr, ErrMetadataNotFound) {
		return nil, metaErr
	}
	if metaErr == nil && meta.UseQuant != s.useQuant {
		return nil, fmt.Errorf("%w: collection %q was created with a different quantization setting", ErrInvalidConfig, colName)
	}

	points := make(map[string]*PointStruct)

	err := s.view(func(tx *bbolt.Tx) error {
		b, err := s.collectionBucket(tx, colName)
		if err != nil {
			return err
		}
		if b == nil {
			return nil
		}

		return b.ForEach(func(k, v []byte) error {
			p, err := s.decodePointValue(v)
			if err != nil {
				return fmt.Errorf("failed to decode point %q: %w", k, err)
			}
			points[p.ID] = p
			return nil
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to load points from bbolt: %w", err)
	}
	return points, nil
}

// decodePointValue decrypts and decodes one stored point value, dequantizing
// the vector when the store uses quantization.
func (s *Store) decodePointValue(raw []byte) (*PointStruct, error) {
	plain, err := s.openBytes(raw)
	if err != nil {
		return nil, err
	}
	stored, err := decodeStoredPoint(plain)
	if err != nil {
		return nil, err
	}
	point := fromStoredPoint(&stored)
	if s.useQuant && s.quantizer != nil {
		if quantizedData, ok := point.Payload["__quantized_vector"]; ok {
			if data, ok := quantizedData.([]byte); ok {
				point.Vector = s.quantizer.Dequantize(data)
				delete(point.Payload, "__quantized_vector")
			}
		}
	}
	return point, nil
}

// GetPoint returns the point with the given ID from the collection's durable
// storage. It reads bbolt, so it does not see upserts still buffered in a
// loaded Collection; use Collection.Get for the in-memory view. It returns
// ErrPointNotFound when the point is absent and ErrCollectionNotFound when the
// collection does not exist.
func (s *Store) GetPoint(colName, id string) (*PointStruct, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}

	var point *PointStruct
	err := s.view(func(tx *bbolt.Tx) error {
		b, err := s.collectionBucket(tx, colName)
		if err != nil {
			return err
		}
		if b == nil {
			return ErrCollectionNotFound
		}
		key, err := s.keyBytes(id)
		if err != nil {
			return err
		}
		raw := b.Get(key)
		if raw == nil {
			return ErrPointNotFound
		}
		point, err = s.decodePointValue(raw)
		return err
	})
	if err != nil {
		return nil, err
	}
	return point, nil
}

// GetPoints returns the points with the given IDs from a collection's durable
// storage. found maps each present ID to its point; missing lists, in request
// order, the IDs that were absent. Duplicate requested IDs are returned once.
// It returns ErrCollectionNotFound when the collection does not exist.
func (s *Store) GetPoints(colName string, ids []string) (found map[string]*PointStruct, missing []string, err error) {
	if err := s.checkOpen(); err != nil {
		return nil, nil, err
	}

	found = make(map[string]*PointStruct, len(ids))
	seen := make(map[string]struct{}, len(ids))
	err = s.view(func(tx *bbolt.Tx) error {
		b, err := s.collectionBucket(tx, colName)
		if err != nil {
			return err
		}
		if b == nil {
			return ErrCollectionNotFound
		}
		for _, id := range ids {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			key, err := s.keyBytes(id)
			if err != nil {
				return err
			}
			raw := b.Get(key)
			if raw == nil {
				missing = append(missing, id)
				continue
			}
			point, err := s.decodePointValue(raw)
			if err != nil {
				return err
			}
			found[id] = point
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return found, missing, nil
}

// ListCollections returns all collection names (bucket names) in the storage.
// It excludes the internal metadata bucket.
func (s *Store) ListCollections() ([]string, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}

	var collections []string
	addCollection := func(name []byte) error {
		if string(name) == collectionsMetaBucket {
			return nil
		}

		colName, err := s.logicalName(name)
		if err != nil {
			return fmt.Errorf("failed to decrypt collection name %q: %w", name, err)
		}

		collections = append(collections, colName)
		return nil
	}

	err := s.view(func(tx *bbolt.Tx) error {
		if len(s.root) > 0 {
			root := s.rootBucket(tx)
			if root == nil {
				return nil
			}
			return root.ForEach(func(name, value []byte) error {
				if value != nil {
					return nil // keys are not collections
				}
				return addCollection(name)
			})
		}
		return tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
			if reservedTopLevelBuckets[string(name)] {
				return nil
			}
			return addCollection(name)
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list collections: %w", err)
	}
	return collections, nil
}

// SaveCollectionMeta persists collection metadata to a special bucket.
func (s *Store) SaveCollectionMeta(name string, meta CollectionMeta) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	if err := s.update(func(tx *bbolt.Tx) error {
		b, err := s.metaBucket(tx, true)
		if err != nil {
			return err
		}

		data, err := json.Marshal(meta)
		if err != nil {
			return err
		}

		sealed, err := s.sealBytes(data)
		if err != nil {
			return fmt.Errorf("failed to encrypt metadata for %s: %w", name, err)
		}

		key, err := s.bucketKey(name)
		if err != nil {
			return fmt.Errorf("failed to derive the metadata key for %q: %w", name, err)
		}

		return b.Put(key, sealed)
	}); err != nil {
		return err
	}
	raw, _ := json.Marshal(meta)
	return s.auditEntry("save_collection_meta", name, digestOf(raw))
}

// LoadCollectionMeta retrieves collection metadata from the special bucket.
func (s *Store) LoadCollectionMeta(name string) (*CollectionMeta, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}

	var meta CollectionMeta
	err := s.view(func(tx *bbolt.Tx) error {
		b, err := s.metaBucket(tx, false)
		if err != nil {
			return err
		}
		if b == nil {
			return ErrMetadataNotFound
		}

		key, err := s.bucketKey(name)
		if err != nil {
			return fmt.Errorf("failed to derive the metadata key for %q: %w", name, err)
		}

		data := b.Get(key)
		if data == nil {
			return fmt.Errorf("%w: %s", ErrMetadataNotFound, name)
		}

		plain, err := s.openBytes(data)
		if err != nil {
			return fmt.Errorf("failed to decrypt metadata for %s: %w", name, err)
		}

		if err := json.Unmarshal(plain, &meta); err != nil {
			return err
		}
		if meta.FormatVersion > CollectionFormatVersion {
			return fmt.Errorf("%w: collection %q has format version %d, this build supports %d", ErrInvalidConfig, name, meta.FormatVersion, CollectionFormatVersion)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return &meta, nil
}

// ListCollectionMetas returns all collection metadata stored in the database.
func (s *Store) ListCollectionMetas() ([]CollectionMeta, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}

	var metas []CollectionMeta
	err := s.view(func(tx *bbolt.Tx) error {
		b, err := s.metaBucket(tx, false)
		if err != nil {
			return err
		}
		if b == nil {
			return nil // No metadata stored yet
		}

		return b.ForEach(func(k, v []byte) error {
			if string(k) == markerKey {
				return nil // the encryption marker is not collection metadata
			}

			plain, err := s.openBytes(v)
			if err != nil {
				return fmt.Errorf("failed to decrypt metadata %q: %w", k, err)
			}

			var meta CollectionMeta
			if err := json.Unmarshal(plain, &meta); err != nil {
				return err
			}
			metas = append(metas, meta)
			return nil
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list collection metas: %w", err)
	}
	return metas, nil
}

// DeletePoints deletes a batch of points from disk by their IDs.
// If a point ID doesn't exist, it is silently skipped.
// Returns an error if the collection doesn't exist.
func (s *Store) DeletePoints(colName string, ids []string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	if err := s.update(func(tx *bbolt.Tx) error {
		b, err := s.collectionBucket(tx, colName)
		if err != nil {
			return err
		}
		if b == nil {
			return nil // Bucket doesn't exist, nothing to delete
		}

		for _, id := range ids {
			key, err := s.keyBytes(id)
			if err != nil {
				return fmt.Errorf("failed to derive the key for point %q: %w", id, err)
			}
			if err := b.Delete(key); err != nil {
				return fmt.Errorf("failed to delete point %s from bbolt: %w", id, err)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return s.auditEntry("delete", colName, nil)
}

// DropCollection removes a collection and its metadata from the storage.
func (s *Store) DropCollection(name string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	key, err := s.bucketKey(name)
	if err != nil {
		return fmt.Errorf("failed to derive the bucket key for collection %q: %w", name, err)
	}

	if err := s.update(func(tx *bbolt.Tx) error {
		// Delete collection bucket under the configured root.
		if len(s.root) > 0 {
			if root := s.rootBucket(tx); root != nil {
				if err := root.DeleteBucket(key); err != nil && err != bolterrors.ErrBucketNotFound {
					return fmt.Errorf("failed to delete bucket: %w", err)
				}
			}
		} else if err := tx.DeleteBucket(key); err != nil && err != bolterrors.ErrBucketNotFound {
			return fmt.Errorf("failed to delete bucket: %w", err)
		}

		// Delete metadata.
		metaBucket, err := s.metaBucket(tx, false)
		if err != nil {
			return err
		}
		if metaBucket != nil {
			if err := metaBucket.Delete(key); err != nil {
				return fmt.Errorf("failed to delete metadata: %w", err)
			}
		}

		return nil
	}); err != nil {
		return err
	}
	if s.tx != nil {
		if s.dropped == nil {
			s.dropped = make(map[string]struct{})
		}
		s.dropped[name] = struct{}{}
	}
	return s.auditEntry("drop_collection", name, nil)
}
