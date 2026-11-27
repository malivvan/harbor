// Package tempest exposes a single bbolt-backed store that combines the record,
// fstree, series, vector and other sub-stores behind one password.
//
// The password is stretched with Argon2id using the salt and cost parameters
// kept in the unencrypted system header, then one key is derived per
// sub-store. The password itself is never stored. A sealed canary in the
// Metadata detects a wrong password even for stores created with encryption
// disabled.
//
// The sub-stores are reached through Record, File, Series, Vector, Search,
// Graphs and Objects. Flags reports the persisted encryption flags and Meta
// reports the KDF parameters. Use EncryptNone/Plaintext to create an
// unencrypted store.
package tempest

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/malivvan/tempest/pkg/metrics"
	"github.com/malivvan/tempest/store"
	"github.com/malivvan/tempest/store/fstree"
	"github.com/malivvan/tempest/store/graphs"
	"github.com/malivvan/tempest/store/ledger"
	"github.com/malivvan/tempest/store/object"
	"github.com/malivvan/tempest/store/record"
	"github.com/malivvan/tempest/store/search"
	"github.com/malivvan/tempest/store/series"
	"github.com/malivvan/tempest/store/system"
	"github.com/malivvan/tempest/store/vector"
	bolt "go.etcd.io/bbolt"
)

// namedRoot returns the default one-element bucket path for a sub-store.
func namedRoot(name string) [][]byte {
	return [][]byte{[]byte(name)}
}

// Errors returned by the package.
var (
	// ErrClosed is returned by operations on a closed Store.
	ErrClosed = errors.New("tempest: store is closed")

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
	EncryptNone                  = system.EncryptNone
	EncryptRecordKeys            = system.EncryptRecordKeys
	EncryptRecordValues          = system.EncryptRecordValues
	EncryptRecordMetadata        = system.EncryptRecordMetadata
	EncryptFileContent           = system.EncryptFileContent
	EncryptFileMetadata          = system.EncryptFileMetadata
	EncryptSeriesValues          = system.EncryptSeriesValues
	EncryptSeriesNames           = system.EncryptSeriesNames
	EncryptVectorPointIDs        = system.EncryptVectorPointIDs
	EncryptVectorCollectionNames = system.EncryptVectorCollectionNames
	EncryptVectorPoints          = system.EncryptVectorPoints
	EncryptGraphValues           = system.EncryptGraphValues
	EncryptObjectValues          = system.EncryptObjectValues
	EncryptRecord                = system.EncryptRecord
	EncryptFile                  = system.EncryptFile
	EncryptSeries                = system.EncryptSeries
	EncryptVector                = system.EncryptVector
	EncryptGraph                 = system.EncryptGraph
	EncryptObject                = system.EncryptObject
	EncryptAll                   = system.EncryptAll
	AuditNone                    = system.AuditNone
	AuditRecord                  = system.AuditRecord
	AuditFile                    = system.AuditFile
	AuditSeries                  = system.AuditSeries
	AuditVector                  = system.AuditVector
	AuditGraphs                  = system.AuditGraphs
	AuditObject                  = system.AuditObject
	AuditAll                     = system.AuditAll
	SaltSize                     = system.SaltSize
	KeySize                      = system.KeySize
	MiB                          = system.MiB
	FastKDFTime                  = system.FastKDFTime
	FastKDFMemory                = system.FastKDFMemory
	FastKDFThreads               = system.FastKDFThreads
	DefaultKDFTime               = system.DefaultKDFTime
	DefaultKDFMemory             = system.DefaultKDFMemory
	DefaultKDFThreads            = system.DefaultKDFThreads
	StrongKDFTime                = system.StrongKDFTime
	StrongKDFMemory              = system.StrongKDFMemory
	StrongKDFThreads             = system.StrongKDFThreads
)

// Store is a single bbolt database that hosts the record, fstree, series, vector, search, graphs and object stores.
// Each sub-store uses its own domain-separated key derived from Password.
type Store struct {
	mu sync.RWMutex
	// txMu serializes explicit cross-store write transactions.
	txMu   sync.Mutex
	bs     *bolt.DB
	record *record.Store
	fstree *fstree.Store
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

// Options configures Open.
//
// Password is required and is never stored: it is stretched with Argon2id and
// the result is used to derive one key per sub-store. The bbolt handle is
// configured from DefaultBoltOptions; the fields at the end of the struct
// override the settings a caller may reasonably need, and every other bbolt
// setting is taken from that variable unchanged.
type Options struct {
	// Codec encodes entities for the record sub-store. A nil Codec selects the
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

	// UseQuant enables vector quantization in the vector sub-store.
	UseQuant bool

	// Quantizer compresses vectors when UseQuant is enabled. A nil Quantizer
	// selects the default SQ8 quantizer.
	Quantizer vector.Quantizer

	// Metrics, when non-nil, receives operation counters, latency histograms
	// and tracing spans from every sub-store. A nil Observer disables
	// instrumentation and costs only a nil check per operation.
	//
	// tempest does not depend on a metrics library: implement
	// metrics.Observer to forward to Prometheus, OpenTelemetry or anything
	// else. See the metrics package for the metric names and labels.
	Metrics metrics.Observer

	// Timeout bounds how long Open waits for bbolt's exclusive file lock before
	// failing. A zero Timeout selects DefaultBoltOptions.Timeout (two seconds),
	// not bbolt's wait-forever. A negative value is ignored for the same
	// reason: waiting forever is never a supported mode.
	Timeout time.Duration

	// InitialMmapSize pre-sizes the database mapping in bytes. Zero selects
	// DefaultBoltOptions.InitialMmapSize (grow on demand). Raising it avoids
	// remapping while long read transactions are open, but on Windows bbolt
	// resizes the file to that size immediately.
	InitialMmapSize int

	// MaxSize caps the database file size in bytes. Zero selects
	// DefaultBoltOptions.MaxSize (no cap).
	MaxSize int

	// Mlock pins the database mapping in memory. The default is false, which
	// keeps the mapping reclaimable; locking a growing store can exhaust RAM.
	Mlock bool

	// NoSync disables the fdatasync of each committed transaction. The default
	// is false. Setting it true trades crash durability for write throughput;
	// use it only for data that can be rebuilt after a power loss.
	NoSync bool

	// Logger receives bbolt's internal diagnostics. A nil Logger selects
	// bbolt's discard logger, so tempest emits no database paths, keys or
	// contents by default.
	Logger bolt.Logger
}

// defaultBoltLockTimeout bounds how long Open waits for the exclusive file
// lock. bbolt treats a zero Timeout as "wait forever"; a stale lock would then
// hang the opening process instead of reporting a second instance.
const defaultBoltLockTimeout = 2 * time.Second

// DefaultBoltOptions is the complete bbolt.Options tempest applies when it
// opens the database handle.
//
// Every field is set explicitly, including the ones that keep bbolt's zero
// value, so the defaults do not drift when bbolt changes its own defaults and
// the reason for each value is visible next to it. Crash consistency comes
// first: an acknowledged write must survive a power loss, and a crash mid-write
// must reopen as the last committed transaction. Throughput is the second
// consideration, mostly through the freelist type and by not adding tuning that
// costs memory, file size or portability.
//
// It is a variable so a caller that opens a bbolt handle itself — for example
// to pass the same database to record.Open or fstree.Open — can reuse the
// settings. Treat it as read-only: take a copy before changing a field.
//
//	opts := tempest.DefaultBoltOptions
//	opts.Timeout = time.Minute
//	bdb, err := bolt.Open(path, 0o600, &opts)
//
// Open builds its handle from a copy of this value and applies the corresponding
// tempest.Options fields on top. Fields without such a passthrough (NoGrowSync,
// NoFreelistSync, FreelistType, PreLoadFreelist, MmapFlags, PageSize, OpenFile,
// ReadOnly, NoStatistics) always come from here.
var DefaultBoltOptions = bolt.Options{
	// Timeout: bbolt's zero value waits forever for the exclusive file
	// lock. Two seconds lets another process finish a clean shutdown and
	// still fails a stale or hung lock instead of blocking the agent that
	// owns the memories.
	Timeout: defaultBoltLockTimeout,

	// NoGrowSync: false truncates and fsyncs the file extension before any
	// page is written into it, so a crash cannot leave a mapping over a
	// tail that was never persisted (zero-filled pages, SIGBUS on the next
	// access). It costs one extra sync per growth, not per commit.
	NoGrowSync: false,

	// NoFreelistSync: false writes the free-page list in the same
	// transaction that frees the pages. Reopen then reads one page instead
	// of scanning the whole file to rebuild the list, and a freed page is
	// never reused without a durable record of the free.
	NoFreelistSync: false,

	// PreLoadFreelist: bbolt loads the freelist at open for a writable
	// database regardless. Setting it keeps the struct complete and makes
	// the one-time read at open explicit instead of implied.
	PreLoadFreelist: true,

	// FreelistType: the array freelist scans free spans linearly once a
	// long-lived, frequently updated store fragments, which is the normal
	// shape of an agent's memory store. The hashmap freelist keeps
	// allocation fast, persists the same on-disk format and therefore
	// changes nothing about crash safety.
	FreelistType: bolt.FreelistMapType,

	// ReadOnly: tempest has to create and migrate its system header, so the
	// handle is always writable. A read-only handle fails once
	// LoadOrCreate needs to write and is not a supported Open mode.
	ReadOnly: false,

	// MmapFlags: zero keeps the OS default mapping. MAP_POPULATE is
	// Linux-only and only moves page faults to open time; a wrong or
	// non-portable flag makes the mapping fail or behave differently per
	// platform.
	MmapFlags: 0,

	// InitialMmapSize: zero grows the mapping on demand. Pre-sizing makes
	// read transactions less likely to block writers, but on Windows it
	// resizes the file to that size immediately, so a default would impose
	// an arbitrary file size on every store. Callers holding long read
	// transactions open can raise it themselves.
	InitialMmapSize: 0,

	// PageSize: zero uses the OS page size, which bbolt also records in the
	// file header. Overriding it changes the on-disk layout and makes the
	// store unreadable on a host with a different page size.
	PageSize: 0,

	// MaxSize: zero means no cap. A cap turns "the disk is full" into an
	// opaque write error and can leave a store that cannot grow; disk
	// quotas and monitoring belong outside the database.
	MaxSize: 0,

	// NoSync: false fdatasyncs the data pages, writes the meta page, then
	// fdatasyncs again. That order is what makes a crash leave either the
	// old or the new transaction; NoSync true acknowledges writes that a
	// power loss can still discard.
	NoSync: false,

	// OpenFile: nil selects os.OpenFile, so the mode passed to Open applies
	// and callers can still inject a custom file (test hooks, in-memory or
	// filesystem-level encryption) themselves.
	OpenFile: nil,

	// Mlock: false keeps the mapping reclaimable. Locking a growing
	// multi-gigabyte memory store can exhaust RAM and get the process
	// killed; callers that can afford the RSS opt in.
	Mlock: false,

	// Logger: nil selects bbolt's discard logger. tempest must not emit
	// database paths, keys or contents by default; a caller that wants
	// bbolt's internal diagnostics injects a logger explicitly.
	Logger: nil,

	// NoStatistics: false keeps bbolt's per-transaction and freelist
	// statistics available through Stats. The cost is small next to sealing
	// and fsync, and without them a write-latency regression is hard to
	// explain.
	NoStatistics: false,
}

// boltOptions builds the bbolt.Options for Open from a copy of
// DefaultBoltOptions with the passthrough fields of Options applied.
//
// Overrides are applied only when they ask for something other than the
// default, so the zero value of Options selects DefaultBoltOptions unchanged
// and an explicit choice always wins. The caller's Options is never mutated.
func (o *Options) boltOptions() bolt.Options {
	opts := DefaultBoltOptions
	if o == nil {
		return opts
	}
	if o.Timeout > 0 {
		opts.Timeout = o.Timeout
	}
	if o.InitialMmapSize > 0 {
		opts.InitialMmapSize = o.InitialMmapSize
	}
	if o.MaxSize > 0 {
		opts.MaxSize = o.MaxSize
	}
	if o.Mlock {
		opts.Mlock = true
	}
	if o.NoSync {
		opts.NoSync = true
	}
	if o.Logger != nil {
		opts.Logger = o.Logger
	}
	return opts
}

// Validate checks the options that apply when a store is created: the password,
// the Argon2id parameter set, the encryption selection and the audit flags.
// Open ignores the KDF costs and the encryption selection for an existing
// store; this method validates them as they would apply to a new store, so
// callers can check their configuration before creating one.
//
// A nil receiver reports ErrInvalidOptions: there is nothing to validate. Open
// treats a nil Options as all-defaults instead, and then fails on the missing
// password with ErrInvalidPassword.
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

// auditPublicKeys holds the per-store Ed25519 public keys used to verify the
// audit trail.
type auditPublicKeys struct {
	auditRecord ed25519.PublicKey
	auditFSTree ed25519.PublicKey
	auditSeries ed25519.PublicKey
	auditVector ed25519.PublicKey
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
//
// A nil options value is accepted and selects the defaults: the crash-safe
// bbolt.Options from DefaultBoltOptions, the DefaultKDF* costs and EncryptAll.
// The password has no default, so a nil options value still fails with
// ErrInvalidPassword. The caller's Options is never mutated; the bbolt handle
// is configured from a copy.
func Open(ctx context.Context, path string, mode os.FileMode, options *Options) (*Store, error) {
	// Copy so a shared or reused Options keeps its configured values across
	// calls.
	effective := Options{}
	if options != nil {
		effective = *options
	}
	options = &effective

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

	bboltOpts := options.boltOptions()
	bdb, err := bolt.Open(path, mode, &bboltOpts)
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
		if system.HasFlag(options.Audit, AuditRecord) {
			recKeys[ledger.StoreRecord] = append(ed25519.PrivateKey(nil), keys.AuditRecord...)
		}
		if system.HasFlag(options.Audit, AuditFile) {
			recKeys[ledger.StoreFile] = append(ed25519.PrivateKey(nil), keys.AuditFile...)
		}
		if system.HasFlag(options.Audit, AuditSeries) {
			recKeys[ledger.StoreSeries] = append(ed25519.PrivateKey(nil), keys.AuditSeries...)
		}
		if system.HasFlag(options.Audit, AuditVector) {
			recKeys[ledger.StoreVector] = append(ed25519.PrivateKey(nil), keys.AuditVector...)
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
			auditRecord: publicOf(keys.AuditRecord),
			auditFSTree: publicOf(keys.AuditFile),
			auditSeries: publicOf(keys.AuditSeries),
			auditVector: publicOf(keys.AuditVector),
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
		Root:            namedRoot(store.RECORD),
		Codec:           options.Codec,
		Secret:          keys.Record,
		EncryptKeys:     system.HasFlag(md.Flags, EncryptRecordKeys),
		EncryptValues:   system.HasFlag(md.Flags, EncryptRecordValues),
		EncryptMetadata: system.HasFlag(md.Flags, EncryptRecordMetadata),
		Audit:           auditSink(AuditRecord),
		Metrics:         options.Metrics,
	}); err != nil {
		return nil, fmt.Errorf("tempest: open record store: %w", err)
	}
	if s.fstree, err = fstree.Open(ctx, bdb, fstree.Config{
		Root:            namedRoot(store.FSTREE),
		Secret:          keys.File,
		EncryptMetadata: system.HasFlag(md.Flags, EncryptFileMetadata),
		EncryptContent:  system.HasFlag(md.Flags, EncryptFileContent),
		Audit:           auditSink(AuditFile),
		Metrics:         options.Metrics,
	}); err != nil {
		return nil, fmt.Errorf("tempest: open file store: %w", err)
	}
	if s.series, err = series.Open(ctx, bdb, series.Config{
		Root:               namedRoot(store.SERIES),
		Secret:             keys.Series,
		EncryptValues:      system.HasFlag(md.Flags, EncryptSeriesValues),
		EncryptSeriesNames: system.HasFlag(md.Flags, EncryptSeriesNames),
		Audit:              auditSink(AuditSeries),
		Metrics:            options.Metrics,
	}); err != nil {
		return nil, fmt.Errorf("tempest: open series store: %w", err)
	}
	if s.search, err = search.Open(ctx, bdb, search.Config{
		Root:          namedRoot(store.SEARCH),
		Secret:        keys.Search,
		EncryptValues: system.HasFlag(md.Flags, EncryptRecordValues),
		Metrics:       options.Metrics,
	}); err != nil {
		return nil, fmt.Errorf("tempest: open search store: %w", err)
	}
	if s.graphs, err = graphs.Open(ctx, bdb, graphs.Config{
		Root:   namedRoot(store.GRAPHS),
		Secret: keys.Graphs,
		// Graph values follow the graph flag for new stores and the db value
		// flag for stores created before the graph flag existed.
		EncryptValues: system.HasFlag(md.Flags, EncryptGraphValues) || system.HasFlag(md.Flags, EncryptRecordValues),
		Audit:         auditSink(AuditGraphs),
		Metrics:       options.Metrics,
	}); err != nil {
		return nil, fmt.Errorf("tempest: open graphs store: %w", err)
	}
	if s.object, err = object.Open(ctx, bdb, object.Config{
		Root:          namedRoot(store.OBJECT),
		Secret:        keys.Object,
		EncryptValues: system.HasFlag(md.Flags, EncryptObjectValues) || system.HasFlag(md.Flags, EncryptRecordValues),
		Audit:         auditSink(AuditObject),
		Metrics:       options.Metrics,
	}); err != nil {
		return nil, fmt.Errorf("tempest: open object store: %w", err)
	}
	if s.vector, err = vector.Open(ctx, bdb, vector.Config{
		Root:                   namedRoot(store.VECTOR),
		Secret:                 keys.Vector,
		UseQuant:               options.UseQuant,
		Quantizer:              options.Quantizer,
		EncryptPoints:          system.HasFlag(md.Flags, EncryptVectorPoints),
		EncryptPointIDs:        system.HasFlag(md.Flags, EncryptVectorPointIDs),
		EncryptCollectionNames: system.HasFlag(md.Flags, EncryptVectorCollectionNames),
		Audit:                  auditSink(AuditVector),
		Metrics:                options.Metrics,
		// The vector store shares this handle with db, fs and ts; Close must only
		// flush its buffers and never close the handle.
		SharedDB: true,
	}); err != nil {
		return nil, fmt.Errorf("tempest: open vector store: %w", err)
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
// view of the same store instead of the conventional root (store.RECORD); no
// segments keeps the conventional root so existing databases stay readable.
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

// File returns the fstree sub-store. Extra path segments select a rooted view
// instead of the conventional root (store.FSTREE).
func (s *Store) File(path ...string) *fstree.Store {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil || s.fstree == nil {
		return nil
	}
	if len(path) == 0 {
		return s.fstree
	}
	return s.fstree.WithRoot(path...)
}

// Series returns the time-series sub-store. Extra path segments select a
// rooted view instead of the conventional root (store.SERIES).
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

// Vector returns the vector sub-store. Extra path segments select a rooted
// view instead of the conventional root (store.VECTOR).
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
// rooted view instead of the conventional root (store.OBJECT).
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
// rooted view instead of the conventional root (store.GRAPHS).
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
// view instead of the conventional root (store.SEARCH).
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
	if system.HasFlag(s.auditFlags, AuditRecord) {
		keys[ledger.StoreRecord] = s.auditPubs.auditRecord
	}
	if system.HasFlag(s.auditFlags, AuditFile) {
		keys[ledger.StoreFile] = s.auditPubs.auditFSTree
	}
	if system.HasFlag(s.auditFlags, AuditSeries) {
		keys[ledger.StoreSeries] = s.auditPubs.auditSeries
	}
	if system.HasFlag(s.auditFlags, AuditVector) {
		keys[ledger.StoreVector] = s.auditPubs.auditVector
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
// fstree destination.
func (s *Store) Backup(ctx context.Context, w io.Writer) error {
	if w == nil {
		return fmt.Errorf("tempest: backup: %w: writer is required", ErrInvalidOptions)
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

	// The vector store buffers points in memory; drain them so the snapshot is
	// consistent with what the store has accepted.
	if s.vector != nil {
		if err := s.vector.Flush(); err != nil && !errors.Is(err, vector.ErrClosed) {
			return fmt.Errorf("tempest: backup: flush vec store: %w", err)
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
			return fmt.Errorf("tempest: backup: %w", err)
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
// bytes go to a temporary fstree in the same directory, are synced and closed,
// and only then replace path by rename. A failed or canceled backup therefore
// never truncates or corrupts an existing backup at path, and no partial fstree
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

// compactTxMaxSize bounds each rewrite transaction during compaction. Committing
// periodically keeps memory bounded for large stores.
const compactTxMaxSize = 64 << 10

// CompactTo writes a compacted copy of the database to path.
//
// bbolt appends pages and only reuses the ones it has freed, so a store that
// grows and shrinks keeps a fstree larger than its live data needs. CompactTo
// rewrites the live pages into a fresh fstree, dropping free space.
//
// Buffered vector points are flushed first. path must not exist; it is created
// with mode (0600 when mode is zero) and removed again if compaction fails.
// Writers may continue during the copy because bbolt compacts from a read
// transaction; Close waits for the copy to finish.
//
// The result is the on-disk representation, not a logical export: an encrypted
// store yields an encrypted compacted fstree that opens with the same password.
// bbolt.Compact cannot be interrupted once started, so ctx is checked before the
// copy only.
func (s *Store) CompactTo(ctx context.Context, path string, mode os.FileMode) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if path == "" {
		return fmt.Errorf("tempest: compact: %w: path is required", ErrInvalidOptions)
	}
	if s == nil {
		return ErrClosed
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil {
		return ErrClosed
	}

	// Buffered vector points are not in the database yet; drain them so the
	// copy is complete.
	if s.vector != nil {
		if err := s.vector.Flush(); err != nil && !errors.Is(err, vector.ErrClosed) {
			return fmt.Errorf("tempest: compact: flush vec store: %w", err)
		}
	}

	if _, err := os.Stat(path); err == nil {
		return &os.PathError{Op: "compact", Path: path, Err: os.ErrExist}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("tempest: compact: %w", err)
	}

	if mode == 0 {
		mode = 0o600
	}
	dst, err := bolt.Open(path, mode, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return fmt.Errorf("tempest: compact: %w", err)
	}
	compactErr := bolt.Compact(dst, s.bs, compactTxMaxSize)
	closeErr := dst.Close()
	if compactErr == nil {
		compactErr = closeErr
	}
	if compactErr != nil {
		_ = os.Remove(path)
		return fmt.Errorf("tempest: compact: %w", compactErr)
	}
	return nil
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
	// The vector store buffers points in memory; drain them before closing the
	// shared handle. A flush failure must not keep the bbolt handle (and its
	// fstree lock) open forever: close it anyway and return the flush error.
	var flushErr error
	if s.vector != nil {
		if err := s.vector.Flush(); err != nil && !errors.Is(err, vector.ErrClosed) {
			flushErr = fmt.Errorf("tempest: flush vec store: %w", err)
		}
	}
	closeErr := s.bs.Close()
	s.bs = nil
	s.record = nil
	s.fstree = nil
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
