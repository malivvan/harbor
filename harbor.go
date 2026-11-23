// Package harbor exposes a single bbolt-backed store that combines the db, fs,
// ts and vec sub-stores behind one password.
//
// The password is stretched with Argon2id using the salt and cost parameters
// kept in an unencrypted "harbor.Metadata" record, then one key is derived per
// sub-store. The password itself is never stored. A sealed canary in the
// Metadata detects a wrong password even for stores created with encryption
// disabled.
//
// The sub-stores are reached through DB, FS, TS, Vec and FTS. Flags reports the
// persisted encryption flags and Meta reports the KDF parameters. Use
// EncryptNone/Plaintext to create an unencrypted store.
package harbor

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/malivvan/harbor/pkg/ledger"
	"github.com/malivvan/harbor/pkg/metrics"
	"github.com/malivvan/harbor/store/fstree"
	"github.com/malivvan/harbor/store/graphs"
	"github.com/malivvan/harbor/store/object"
	"github.com/malivvan/harbor/store/record"
	"github.com/malivvan/harbor/store/search"
	"github.com/malivvan/harbor/store/series"
	"github.com/malivvan/harbor/store/system"
	"github.com/malivvan/harbor/store/vector"
	bolt "go.etcd.io/bbolt"
)

// namedRoot returns the default one-element bucket path for a sub-store.
func namedRoot(name string) [][]byte {
	return [][]byte{[]byte(name)}
}

// Errors returned by the package.
var (
	// ErrClosed is returned by operations on a closed Store.
	ErrClosed = errors.New("harbor: store is closed")

	// ErrInvalidOptions reports a nil Options value or an unusable
	// configuration.
	ErrInvalidOptions = system.ErrInvalidOptions

	// ErrInvalidPassword reports an empty password or a password that does not
	// decrypt the store canary.
	ErrInvalidPassword = system.ErrInvalidPassword
)

// ErrCorruptStore reports missing or malformed store Metadata.
var ErrCorruptStore = system.ErrCorruptStore

// Metadata is the unencrypted store configuration owned by store/system.
type Metadata = system.Metadata

// Encryption, audit and KDF identifiers are re-exported from store/system.
const (
	EncryptNone               = system.EncryptNone
	EncryptDBKeys             = system.EncryptDBKeys
	EncryptDBValues           = system.EncryptDBValues
	EncryptDBMetadata         = system.EncryptDBMetadata
	EncryptFSContent          = system.EncryptFSContent
	EncryptFSMetadata         = system.EncryptFSMetadata
	EncryptTSValues           = system.EncryptTSValues
	EncryptTSSeriesNames      = system.EncryptTSSeriesNames
	EncryptVecPointIDs        = system.EncryptVecPointIDs
	EncryptVecCollectionNames = system.EncryptVecCollectionNames
	EncryptVecPoints          = system.EncryptVecPoints
	EncryptGraphValues        = system.EncryptGraphValues
	EncryptObjectValues       = system.EncryptObjectValues
	EncryptDB                 = system.EncryptDB
	EncryptFS                 = system.EncryptFS
	EncryptTS                 = system.EncryptTS
	EncryptVec                = system.EncryptVec
	EncryptGraph              = system.EncryptGraph
	EncryptObject             = system.EncryptObject
	EncryptAll                = system.EncryptAll
	AuditNone                 = system.AuditNone
	AuditDB                   = system.AuditDB
	AuditFS                   = system.AuditFS
	AuditTS                   = system.AuditTS
	AuditVec                  = system.AuditVec
	AuditGraphs               = system.AuditGraphs
	AuditObject               = system.AuditObject
	AuditAll                  = system.AuditAll
	SaltSize                  = system.SaltSize
	KeySize                   = system.KeySize
	MiB                       = system.MiB
	FastKDFTime               = system.FastKDFTime
	FastKDFMemory             = system.FastKDFMemory
	FastKDFThreads            = system.FastKDFThreads
	DefaultKDFTime            = system.DefaultKDFTime
	DefaultKDFMemory          = system.DefaultKDFMemory
	DefaultKDFThreads         = system.DefaultKDFThreads
	StrongKDFTime             = system.StrongKDFTime
	StrongKDFMemory           = system.StrongKDFMemory
	StrongKDFThreads          = system.StrongKDFThreads
)

// Options configures Open.
//
// Password is required and is never stored: it is stretched with Argon2id and
// the result is used to derive one key per sub-store. The embedded
// bolt.Options configure the underlying database handle.
type Options struct {
	bolt.Options

	// Codec encodes entities for the db sub-store. A nil Codec selects the
	// default JSON codec.
	Codec record.Codec

	// Password protects the store. It is required.
	//
	// Go strings are immutable, so the password cannot be reliably wiped from
	// memory; only the temporary []byte copy used for key derivation is
	// zeroed. Callers that need stronger erasure should keep the password out
	// of long-lived process memory themselves.
	Password string

	// KDFTime, KDFMemory (KiB) and KDFThreads select the Argon2id cost
	// parameters for a new store. All zero selects the DefaultKDF* values; a
	// partial set is rejected. They are ignored when opening an existing
	// store, whose stored parameters always win.
	KDFTime    uint32
	KDFMemory  uint32
	KDFThreads uint8

	// Encrypt selects which components a new store encrypts. The zero value
	// enables EncryptAll. Use Plaintext to create an unencrypted store. It is
	// ignored when opening an existing store.
	Encrypt uint16

	// Plaintext creates a new store without crypto. It conflicts with a
	// non-zero Encrypt. It is ignored when opening an existing store.
	Plaintext bool

	// Audit selects which sub-stores record a tamper-evident audit trail. The
	// zero value (AuditNone) disables it. It is a per-open runtime choice, not
	// persisted; see the Audit* flag constants in the package docs.
	Audit uint16

	// UseQuant enables vector quantization in the vec sub-store.
	UseQuant bool

	// Quantizer compresses vectors when UseQuant is enabled. A nil Quantizer
	// selects the default SQ8 quantizer.
	Quantizer vector.Quantizer

	// Metrics, when non-nil, receives operation counters, latency histograms
	// and tracing spans from every sub-store. A nil Observer disables
	// instrumentation and costs only a nil check per operation.
	//
	// Harbor does not depend on a metrics library: implement
	// metrics.Observer to forward to Prometheus, OpenTelemetry or anything
	// else. See the metrics package for the metric names and labels.
	Metrics metrics.Observer
}

// Validate checks the options that apply when a store is created: the password,
// the Argon2id parameter set, the encryption selection and the audit flags.
// Open ignores the KDF costs and the encryption selection for an existing
// store; this method validates them as they would apply to a new file, so
// callers can check their configuration before creating one.
func (o *Options) Validate() error {
	if o == nil {
		return fmt.Errorf("%w: options are required", ErrInvalidOptions)
	}
	if err := o.systemConfig().Validate(); err != nil {
		return err
	}
	return o.validateAuditFlags()
}

// systemConfig maps the root options onto the header package's configuration.
func (o *Options) systemConfig() system.Config {
	if o == nil {
		return system.Config{}
	}
	return system.Config{
		Password:   o.Password,
		KDFTime:    o.KDFTime,
		KDFMemory:  o.KDFMemory,
		KDFThreads: o.KDFThreads,
		Encrypt:    o.Encrypt,
		Plaintext:  o.Plaintext,
		Audit:      o.Audit,
	}
}

// validateAuditFlags rejects audit bits that do not name a sub-store. Open
// applies the same check, because audit flags are a per-open runtime choice
// even for an existing store.
func (o *Options) validateAuditFlags() error {
	if unknown := o.Audit &^ AuditAll; unknown != 0 {
		return fmt.Errorf("%w: unknown audit flags %#x", ErrInvalidOptions, unknown)
	}
	return nil
}

// Store is a single bbolt database that hosts the db, fs, ts, vec and fts stores.
// Each sub-store uses its own domain-separated key derived from Password.
type Store struct {
	mu sync.RWMutex
	// txMu serializes explicit cross-store write transactions.
	txMu   sync.Mutex
	bs     *bolt.DB
	record *record.Store
	file   *fstree.Store
	series *series.Store
	vector *vector.Store
	search *search.Store
	graphs *graphs.Store
	object *object.Store

	// audit records events for the enabled stores, or nil when disabled.
	audit *ledger.Recorder
	// auditFlags remembers which stores are enabled, for verification.
	auditFlags uint16
	// auditPubs holds the public keys matching the recorder's private keys, so
	// VerifyAudit can check the chain without re-deriving from the password.
	auditPubs auditPublicKeys
}

// auditPublicKeys holds the per-store Ed25519 public keys used to verify the
// audit trail.
type auditPublicKeys struct {
	auditDB     ed25519.PublicKey
	auditFS     ed25519.PublicKey
	auditTS     ed25519.PublicKey
	auditVec    ed25519.PublicKey
	auditGraphs ed25519.PublicKey
	auditObject ed25519.PublicKey
}

// Open opens (or creates) a Store at path.
//
// The first Open creates unencrypted Metadata (salt, Argon2id parameters,
// encryption flags and a sealed canary) and initializes the key/value bucket.
// Later Opens use the stored settings; the KDF and encryption fields in options
// only apply to a new store. A wrong or empty password fails with
// ErrInvalidPassword and leaves the database closed.
func Open(ctx context.Context, path string, mode os.FileMode, options *Options) (*Store, error) {
	if options == nil {
		return nil, fmt.Errorf("%w: options are required", ErrInvalidOptions)
	}
	if options.Password == "" {
		return nil, fmt.Errorf("%w: password is required", ErrInvalidPassword)
	}
	if err := options.validateAuditFlags(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	bdb, err := bolt.Open(path, mode, &options.Options)
	if err != nil {
		return nil, err
	}
	opened := false
	defer func() {
		if !opened {
			_ = bdb.Close()
		}
	}()

	md, keys, err := system.LoadOrCreate(bdb, options.systemConfig())
	if err != nil {
		return nil, err
	}
	defer keys.Wipe()

	s := &Store{bs: bdb}

	// Build the audit recorder for the enabled stores, copying the derived
	// private keys so they survive storeKeys.wipe below.
	if options.Audit != 0 {
		recKeys := make(map[string]ed25519.PrivateKey, 4)
		if system.HasFlag(options.Audit, AuditDB) {
			recKeys[ledger.StoreDB] = append(ed25519.PrivateKey(nil), keys.AuditDB...)
		}
		if system.HasFlag(options.Audit, AuditFS) {
			recKeys[ledger.StoreFS] = append(ed25519.PrivateKey(nil), keys.AuditFS...)
		}
		if system.HasFlag(options.Audit, AuditTS) {
			recKeys[ledger.StoreTS] = append(ed25519.PrivateKey(nil), keys.AuditTS...)
		}
		if system.HasFlag(options.Audit, AuditVec) {
			recKeys[ledger.StoreVec] = append(ed25519.PrivateKey(nil), keys.AuditVec...)
		}
		if system.HasFlag(options.Audit, AuditGraphs) {
			recKeys[ledger.StoreGraphs] = append(ed25519.PrivateKey(nil), keys.AuditGraphs...)
		}
		if system.HasFlag(options.Audit, AuditObject) {
			recKeys[ledger.StoreObject] = append(ed25519.PrivateKey(nil), keys.AuditObject...)
		}
		s.audit = ledger.NewRecorder(bdb, recKeys)
		s.auditFlags = options.Audit
		s.auditPubs = auditPublicKeys{
			auditDB:     publicOf(keys.AuditDB),
			auditFS:     publicOf(keys.AuditFS),
			auditTS:     publicOf(keys.AuditTS),
			auditVec:    publicOf(keys.AuditVec),
			auditGraphs: publicOf(keys.AuditGraphs),
			auditObject: publicOf(keys.AuditObject),
		}
	}
	auditSink := func(flag uint16) ledger.Sink {
		if s.audit == nil || !system.HasFlag(options.Audit, flag) {
			return nil
		}
		return s.audit
	}

	// Bind each sub-store to its own derived key and explicit bucket root.
	// The persisted flags decide which features are encrypted, so the on-disk
	// mode cannot drift between runs.
	if s.record, err = record.Open(ctx, bdb, record.Config{
		Root:            namedRoot(record.Name),
		Codec:           options.Codec,
		Secret:          keys.Record,
		EncryptKeys:     system.HasFlag(md.Flags, EncryptDBKeys),
		EncryptValues:   system.HasFlag(md.Flags, EncryptDBValues),
		EncryptMetadata: system.HasFlag(md.Flags, EncryptDBMetadata),
		Audit:           auditSink(AuditDB),
		Metrics:         options.Metrics,
	}); err != nil {
		return nil, fmt.Errorf("harbor: open db store: %w", err)
	}
	if s.file, err = fstree.Open(ctx, bdb, fstree.Config{
		Root:            namedRoot(fstree.Name),
		Secret:          keys.File,
		EncryptMetadata: system.HasFlag(md.Flags, EncryptFSMetadata),
		EncryptContent:  system.HasFlag(md.Flags, EncryptFSContent),
		Audit:           auditSink(AuditFS),
		Metrics:         options.Metrics,
	}); err != nil {
		return nil, fmt.Errorf("harbor: open fs store: %w", err)
	}
	if s.series, err = series.Open(ctx, bdb, series.Config{
		Root:               namedRoot(series.Name),
		Secret:             keys.Series,
		EncryptValues:      system.HasFlag(md.Flags, EncryptTSValues),
		EncryptSeriesNames: system.HasFlag(md.Flags, EncryptTSSeriesNames),
		Audit:              auditSink(AuditTS),
		Metrics:            options.Metrics,
	}); err != nil {
		return nil, fmt.Errorf("harbor: open ts store: %w", err)
	}
	if s.search, err = search.Open(ctx, bdb, search.Config{
		Root:          namedRoot(search.Name),
		Secret:        keys.Search,
		EncryptValues: system.HasFlag(md.Flags, EncryptDBValues),
		Metrics:       options.Metrics,
	}); err != nil {
		return nil, fmt.Errorf("harbor: open fts store: %w", err)
	}
	if s.graphs, err = graphs.Open(ctx, bdb, graphs.Config{
		Root:   namedRoot(graphs.Name),
		Secret: keys.Graphs,
		// Graph values follow the graph flag for new stores and the db value
		// flag for stores created before the graph flag existed.
		EncryptValues: system.HasFlag(md.Flags, EncryptGraphValues) || system.HasFlag(md.Flags, EncryptDBValues),
		Audit:         auditSink(AuditGraphs),
		Metrics:       options.Metrics,
	}); err != nil {
		return nil, fmt.Errorf("harbor: open graphs store: %w", err)
	}
	if s.object, err = object.Open(ctx, bdb, object.Config{
		Root:          namedRoot(object.Name),
		Secret:        keys.Object,
		EncryptValues: system.HasFlag(md.Flags, EncryptObjectValues) || system.HasFlag(md.Flags, EncryptDBValues),
		Audit:         auditSink(AuditObject),
		Metrics:       options.Metrics,
	}); err != nil {
		return nil, fmt.Errorf("harbor: open object store: %w", err)
	}
	if s.vector, err = vector.Open(ctx, bdb, vector.Config{
		Root:                   namedRoot(vector.Name),
		Secret:                 keys.Vector,
		UseQuant:               options.UseQuant,
		Quantizer:              options.Quantizer,
		EncryptPoints:          system.HasFlag(md.Flags, EncryptVecPoints),
		EncryptPointIDs:        system.HasFlag(md.Flags, EncryptVecPointIDs),
		EncryptCollectionNames: system.HasFlag(md.Flags, EncryptVecCollectionNames),
		Audit:                  auditSink(AuditVec),
		Metrics:                options.Metrics,
		// The vec store shares this handle with db, fs and ts; Close must only
		// flush its buffers and never close the handle.
		SharedDB: true,
	}); err != nil {
		return nil, fmt.Errorf("harbor: open vec store: %w", err)
	}

	opened = true
	return s, nil
}

// publicOf returns the Ed25519 public key matching priv. A nil private key
// yields a nil public key, so a disabled store contributes nothing to verify.
func publicOf(priv ed25519.PrivateKey) ed25519.PublicKey {
	if len(priv) != ed25519.PrivateKeySize {
		return nil
	}
	return append(ed25519.PublicKey(nil), priv.Public().(ed25519.PublicKey)...)
}

// Record returns the record sub-store. Extra path segments select a rooted
// view of the same store instead of the conventional root ("db"); no segments
// keeps the conventional root so existing databases stay readable.
func (s *Store) Record(path ...string) *record.Store {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil || s.record == nil {
		return nil
	}
	if len(path) == 0 {
		return s.record
	}
	return s.record.WithRoot(path...)
}

// File returns the file sub-store. Extra path segments select a rooted view
// instead of the conventional root ("fs").
func (s *Store) File(path ...string) *fstree.Store {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil || s.file == nil {
		return nil
	}
	if len(path) == 0 {
		return s.file
	}
	return s.file.WithRoot(path...)
}

// Series returns the time-series sub-store. Extra path segments select a
// rooted view instead of the conventional root ("ts").
func (s *Store) Series(path ...string) *series.Store {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil || s.series == nil {
		return nil
	}
	if len(path) == 0 {
		return s.series
	}
	return s.series.WithRoot(path...)
}

// Vector returns the vector sub-store. Extra path segments select a rooted view
// instead of the conventional root ("vec").
func (s *Store) Vector(path ...string) *vector.Store {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil || s.vector == nil {
		return nil
	}
	if len(path) == 0 {
		return s.vector
	}
	return s.vector.WithRoot(path...)
}

// Objects returns the flat object sub-store. Extra path segments select a
// rooted view instead of the conventional root ("object").
func (s *Store) Objects(path ...string) *object.Store {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil || s.object == nil {
		return nil
	}
	if len(path) == 0 {
		return s.object
	}
	return s.object.WithRoot(path...)
}

// Graphs returns the property-graph sub-store. Extra path segments select a
// rooted view instead of the conventional root ("graphs").
func (s *Store) Graphs(path ...string) *graphs.Store {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil || s.graphs == nil {
		return nil
	}
	if len(path) == 0 {
		return s.graphs
	}
	return s.graphs.WithRoot(path...)
}

// Search returns the full-text sub-store. Extra path segments select a rooted
// view instead of the conventional root ("fts").
func (s *Store) Search(path ...string) *search.Store {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil || s.search == nil {
		return nil
	}
	if len(path) == 0 {
		return s.search
	}
	return s.search.WithRoot(path...)
}

// Meta returns the persisted salt, Argon2id cost parameters and encryption
// flags. A closed store returns ErrClosed.
func (s *Store) Meta() (salt []byte, time, memory uint32, threads uint8, flags uint16, err error) {
	err = s.withDB(func(bdb *bolt.DB) error {
		md, creating, err := system.ReadMetadata(bdb)
		if err != nil {
			return err
		}
		if creating {
			return fmt.Errorf("%w: missing Metadata", ErrCorruptStore)
		}
		salt, time, memory, threads, flags = md.Salt, md.Time, md.Memory, md.Threads, md.Flags
		return nil
	})
	return
}

// Flags returns the encryption flags persisted by the store. A closed store
// returns EncryptNone.
func (s *Store) Flags() uint16 {
	_, _, _, _, flags, err := s.Meta()
	if err != nil {
		return EncryptNone
	}
	return flags
}

// AuditFlags returns the audit flags the store was opened with. They are a
// per-open runtime choice and are not persisted, so a closed store returns
// AuditNone.
func (s *Store) AuditFlags() uint16 {
	if s == nil {
		return AuditNone
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil {
		return AuditNone
	}
	return s.auditFlags
}

// Audit returns the recorder backing the audit trail, or nil when the store
// was opened with AuditNone. Callers use it to record out-of-band events and
// to anchor the chain tip with Head.
func (s *Store) Audit() *ledger.Recorder {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.audit
}

// VerifyAudit replays the audit trail and checks every signature and chain
// link. It returns the event count and chain tip. Keys are derived from the
// store password, so a store opened with a different password fails
// verification. It returns audit.ErrNoLog when no trail exists.
func (s *Store) VerifyAudit() (ledger.Result, error) {
	if s == nil {
		return ledger.Result{}, ErrClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil {
		return ledger.Result{}, ErrClosed
	}
	keys := make(map[string]ed25519.PublicKey, 4)
	if system.HasFlag(s.auditFlags, AuditDB) {
		keys[ledger.StoreDB] = s.auditPubs.auditDB
	}
	if system.HasFlag(s.auditFlags, AuditFS) {
		keys[ledger.StoreFS] = s.auditPubs.auditFS
	}
	if system.HasFlag(s.auditFlags, AuditTS) {
		keys[ledger.StoreTS] = s.auditPubs.auditTS
	}
	if system.HasFlag(s.auditFlags, AuditVec) {
		keys[ledger.StoreVec] = s.auditPubs.auditVec
	}
	if system.HasFlag(s.auditFlags, AuditGraphs) {
		keys[ledger.StoreGraphs] = s.auditPubs.auditGraphs
	}
	if system.HasFlag(s.auditFlags, AuditObject) {
		keys[ledger.StoreObject] = s.auditPubs.auditObject
	}
	return ledger.Verify(s.bs, keys)
}

// withDB runs fn with the open database while holding the read lock. The lock
// makes Close wait for in-flight operations and turns calls after Close into
// ErrClosed.
func (s *Store) withDB(fn func(*bolt.DB) error) error {
	if s == nil {
		return ErrClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil {
		return ErrClosed
	}
	return fn(s.bs)
}

// View runs fn against a consistent read-only snapshot of the underlying
// database. It is used by the consensus layer to serialize the store for raft
// snapshots and by callers that need a raw bbolt view (for example, a backup).
// fn must not write to the database.
func (s *Store) View(fn func(*bolt.Tx) error) error {
	return s.withDB(func(b *bolt.DB) error { return b.View(fn) })
}

// Backup writes a consistent, point-in-time snapshot of the whole database to
// w.
//
// Buffered vector points are flushed first, so the snapshot contains every
// upsert the store has accepted, including points still waiting for the
// collection flush threshold. The snapshot is taken inside one bbolt read
// transaction: concurrent writers are not blocked and do not appear in it.
// The store lock is held for the duration, so Close waits for a running
// backup and a backup started after Close returns ErrClosed.
//
// The bytes are the on-disk representation of the database. An encrypted store
// produces an encrypted backup: it contains the same markers and ciphertext and
// is opened with the same password. Nothing is decrypted or exported in
// plaintext, so a backup is safe to store next to the original.
//
// The snapshot is complete only when Backup returns nil. A writer failure or a
// canceled context leaves w holding a partial copy; use BackupTo for an atomic
// file destination.
func (s *Store) Backup(ctx context.Context, w io.Writer) error {
	if w == nil {
		return fmt.Errorf("harbor: backup: %w: writer is required", ErrInvalidOptions)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil {
		return ErrClosed
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil {
		return ErrClosed
	}

	// The vec store buffers points in memory; drain them so the snapshot is
	// consistent with what the store has accepted.
	if s.vector != nil {
		if err := s.vector.Flush(); err != nil && !errors.Is(err, vector.ErrClosed) {
			return fmt.Errorf("harbor: backup: flush vec store: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	err := s.bs.View(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := tx.WriteTo(ctxWriter{ctx: ctx, w: w}); err != nil {
			return fmt.Errorf("harbor: backup: %w", err)
		}
		return nil
	})
	if err != nil {
		// bbolt wraps its write errors with %s, which breaks the cause chain;
		// report cancellation directly so callers can match it with errors.Is.
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		return err
	}
	return nil
}

// BackupTo writes the snapshot produced by Backup to path atomically: the
// bytes go to a temporary file in the same directory, are synced and closed,
// and only then replace path by rename. A failed or canceled backup therefore
// never truncates or corrupts an existing backup at path, and no partial file
// is left behind.
func (s *Store) BackupTo(ctx context.Context, path string) error {
	_, err := s.BackupFile(ctx, path, BackupOptions{Overwrite: true})
	return err
}

// ctxWriter aborts a long write (such as a bbolt snapshot) when the context is
// canceled, so a backup cannot outlive its deadline.
type ctxWriter struct {
	ctx context.Context
	w   io.Writer
}

func (cw ctxWriter) Write(p []byte) (int, error) {
	if err := cw.ctx.Err(); err != nil {
		return 0, err
	}
	return cw.w.Write(p)
}

// Close closes the underlying database. It is safe to call more than once, and
// the sub-store accessors return nil afterwards.
//
// The sub-stores borrow this single bbolt handle, so closing it is enough; the
// references are cleared to make accidental use after Close fail predictably.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bs == nil {
		return nil
	}
	// The vec store buffers points in memory; drain them before closing the
	// shared handle. A flush failure must not keep the bbolt handle (and its
	// file lock) open forever: close it anyway and return the flush error.
	var flushErr error
	if s.vector != nil {
		if err := s.vector.Flush(); err != nil && !errors.Is(err, vector.ErrClosed) {
			flushErr = fmt.Errorf("harbor: flush vec store: %w", err)
		}
	}
	closeErr := s.bs.Close()
	s.bs = nil
	s.record = nil
	s.file = nil
	s.series = nil
	s.vector = nil
	s.search = nil
	s.graphs = nil
	s.object = nil
	if flushErr != nil && closeErr != nil {
		return errors.Join(flushErr, closeErr)
	}
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}
