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
	"runtime"
	"sync"

	"github.com/malivvan/harbor/internal/crypto"
	"github.com/malivvan/harbor/internal/slip21"
	"github.com/malivvan/harbor/pkg/audit"
	"github.com/malivvan/harbor/pkg/metrics"
	"github.com/malivvan/harbor/store/file"
	"github.com/malivvan/harbor/store/record"
	"github.com/malivvan/harbor/store/search"
	"github.com/malivvan/harbor/store/series"
	"github.com/malivvan/harbor/store/vector"
	bolt "go.etcd.io/bbolt"
)

// namedRoot returns the default one-element bucket path for a sub-store.
func namedRoot(name string) [][]byte {
	return [][]byte{[]byte(name)}
}

// Domain-separated labels used to derive one key per component.
const (
	labelDB  = "harbor/db/v1"
	labelFS  = "harbor/fs/v1"
	labelTS  = "harbor/ts/v1"
	labelVec = "harbor/vec/v1"
	labelFTS = "harbor/fts/v1"
)

// SLIP-10 (Ed25519) hardened paths for the per-store audit signing keys. They
// mirror the per-store SLIP-21 keys above, hanging off the same Argon2id seed.
const (
	auditPathDB  = "m/0'"
	auditPathFS  = "m/1'"
	auditPathTS  = "m/2'"
	auditPathVec = "m/3'"
)

// Errors returned by the package.
var (
	// ErrClosed is returned by operations on a closed Store.
	ErrClosed = errors.New("harbor: store is closed")

	// ErrInvalidOptions reports a nil Options value or an unusable
	// configuration.
	ErrInvalidOptions = errors.New("harbor: invalid options")

	// ErrInvalidPassword reports an empty password or a password that does not
	// decrypt the store canary.
	ErrInvalidPassword = errors.New("harbor: invalid password")
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
	if o.Password == "" {
		return fmt.Errorf("%w: password is required", ErrInvalidPassword)
	}
	if _, _, _, _, err := o.creationParams(); err != nil {
		return err
	}
	return o.validateAuditFlags()
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

// creationParams resolves the Argon2id cost parameters and encryption flags a
// new store would use, validating the combination. It is the single source of
// truth for new-store validation: Open and Options.Validate both use it.
func (o *Options) creationParams() (time, memory uint32, threads uint8, flags uint16, err error) {
	time, memory, threads = o.KDFTime, o.KDFMemory, o.KDFThreads
	switch {
	case time == 0 && memory == 0 && threads == 0:
		time, memory, threads = DefaultKDFTime, DefaultKDFMemory, DefaultKDFThreads
	case time == 0 || memory == 0 || threads == 0:
		return 0, 0, 0, 0, fmt.Errorf("%w: incomplete KDF parameters", ErrInvalidOptions)
	}
	flags, err = encryptionFlags(o)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if err := validateKDFParams(time, memory, threads, flags, ErrInvalidOptions); err != nil {
		return 0, 0, 0, 0, err
	}
	return time, memory, threads, flags, nil
}

// Store is a single bbolt database that hosts the db, fs, ts, vec and fts stores.
// Each sub-store uses its own domain-separated key derived from Password.
type Store struct {
	mu sync.RWMutex
	// txMu serializes explicit cross-store write transactions.
	txMu   sync.Mutex
	bs     *bolt.DB
	record *record.Store
	file   *file.Store
	series *series.Store
	vector *vector.Store
	search *search.Store

	// audit records events for the enabled stores, or nil when disabled.
	audit *audit.Recorder
	// auditFlags remembers which stores are enabled, for verification.
	auditFlags uint16
	// auditPubs holds the public keys matching the recorder's private keys, so
	// VerifyAudit can check the chain without re-deriving from the password.
	auditPubs auditPublicKeys
}

// auditPublicKeys holds the per-store Ed25519 public keys used to verify the
// audit trail.
type auditPublicKeys struct {
	auditDB  ed25519.PublicKey
	auditFS  ed25519.PublicKey
	auditTS  ed25519.PublicKey
	auditVec ed25519.PublicKey
}

// storeKeys holds the per-component keys derived for one Open call. They are
// wiped as soon as the sub-stores have copied them.
type storeKeys struct {
	canary []byte
	record []byte
	file   []byte
	series []byte
	vector []byte
	search []byte

	auditDB  ed25519.PrivateKey
	auditFS  ed25519.PrivateKey
	auditTS  ed25519.PrivateKey
	auditVec ed25519.PrivateKey
}

func (k *storeKeys) wipe() {
	if k == nil {
		return
	}
	for _, b := range [][]byte{k.canary, k.record, k.file, k.series, k.vector, k.search, k.auditDB, k.auditFS, k.auditTS, k.auditVec} {
		wipe(b)
	}
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

	md, keys, err := loadOrCreateState(bdb, options)
	if err != nil {
		return nil, err
	}
	defer keys.wipe()

	s := &Store{bs: bdb}

	// Build the audit recorder for the enabled stores, copying the derived
	// private keys so they survive storeKeys.wipe below.
	if options.Audit != 0 {
		recKeys := make(map[string]ed25519.PrivateKey, 4)
		if hasFlag(options.Audit, AuditDB) {
			recKeys[audit.StoreDB] = append(ed25519.PrivateKey(nil), keys.auditDB...)
		}
		if hasFlag(options.Audit, AuditFS) {
			recKeys[audit.StoreFS] = append(ed25519.PrivateKey(nil), keys.auditFS...)
		}
		if hasFlag(options.Audit, AuditTS) {
			recKeys[audit.StoreTS] = append(ed25519.PrivateKey(nil), keys.auditTS...)
		}
		if hasFlag(options.Audit, AuditVec) {
			recKeys[audit.StoreVec] = append(ed25519.PrivateKey(nil), keys.auditVec...)
		}
		s.audit = audit.NewRecorder(bdb, recKeys)
		s.auditFlags = options.Audit
		s.auditPubs = auditPublicKeys{
			auditDB:  publicOf(keys.auditDB),
			auditFS:  publicOf(keys.auditFS),
			auditTS:  publicOf(keys.auditTS),
			auditVec: publicOf(keys.auditVec),
		}
	}
	auditSink := func(flag uint16) audit.Sink {
		if s.audit == nil || !hasFlag(options.Audit, flag) {
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
		Secret:          keys.record,
		EncryptKeys:     hasFlag(md.Flags, EncryptDBKeys),
		EncryptValues:   hasFlag(md.Flags, EncryptDBValues),
		EncryptMetadata: hasFlag(md.Flags, EncryptDBMetadata),
		Audit:           auditSink(AuditDB),
		Metrics:         options.Metrics,
	}); err != nil {
		return nil, fmt.Errorf("harbor: open db store: %w", err)
	}
	if s.file, err = file.Open(ctx, bdb, file.Config{
		Root:            namedRoot(file.Name),
		Secret:          keys.file,
		EncryptMetadata: hasFlag(md.Flags, EncryptFSMetadata),
		EncryptContent:  hasFlag(md.Flags, EncryptFSContent),
		Audit:           auditSink(AuditFS),
		Metrics:         options.Metrics,
	}); err != nil {
		return nil, fmt.Errorf("harbor: open fs store: %w", err)
	}
	if s.series, err = series.Open(ctx, bdb, series.Config{
		Root:               namedRoot(series.Name),
		Secret:             keys.series,
		EncryptValues:      hasFlag(md.Flags, EncryptTSValues),
		EncryptSeriesNames: hasFlag(md.Flags, EncryptTSSeriesNames),
		Audit:              auditSink(AuditTS),
		Metrics:            options.Metrics,
	}); err != nil {
		return nil, fmt.Errorf("harbor: open ts store: %w", err)
	}
	if s.search, err = search.Open(ctx, bdb, search.Config{
		Root:          namedRoot(search.Name),
		Secret:        keys.search,
		EncryptValues: hasFlag(md.Flags, EncryptDBValues),
		Metrics:       options.Metrics,
	}); err != nil {
		return nil, fmt.Errorf("harbor: open fts store: %w", err)
	}
	if s.vector, err = vector.Open(ctx, bdb, vector.Config{
		Root:                   namedRoot(vector.Name),
		Secret:                 keys.vector,
		UseQuant:               options.UseQuant,
		Quantizer:              options.Quantizer,
		EncryptPoints:          hasFlag(md.Flags, EncryptVecPoints),
		EncryptPointIDs:        hasFlag(md.Flags, EncryptVecPointIDs),
		EncryptCollectionNames: hasFlag(md.Flags, EncryptVecCollectionNames),
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

// loadOrCreateState reads the Metadata and derives the component keys. A new
// store resolves options into Metadata, seals the canary and persists it; an
// existing store derives keys from the stored parameters and verifies the
// canary. The returned keys must be wiped by the caller.
func loadOrCreateState(bdb *bolt.DB, options *Options) (Metadata, *storeKeys, error) {
	md, creating, err := readMetadata(bdb)
	if err != nil {
		return Metadata{}, nil, err
	}
	if !creating {
		keys, err := deriveKeys(options.Password, md.Salt, md.Time, md.Memory, md.Threads)
		if err != nil {
			return Metadata{}, nil, err
		}
		if err := verifyCanary(keys.canary, md.Canary, md.Flags); err != nil {
			keys.wipe()
			return Metadata{}, nil, fmt.Errorf("%w: %w", ErrInvalidPassword, err)
		}
		return md, keys, nil
	}

	salt, kdfTime, kdfMemory, kdfThreads, flags, err := newKDFParams(options)
	if err != nil {
		return Metadata{}, nil, err
	}
	keys, err := deriveKeys(options.Password, salt, kdfTime, kdfMemory, kdfThreads)
	if err != nil {
		return Metadata{}, nil, err
	}
	canary, err := sealCanary(keys.canary, flags)
	if err != nil {
		keys.wipe()
		return Metadata{}, nil, err
	}
	md = Metadata{
		Salt:    salt,
		Time:    kdfTime,
		Flags:   flags,
		Memory:  kdfMemory,
		Threads: kdfThreads,
		Canary:  canary,
	}
	if err := writeMetadata(bdb, md); err != nil {
		keys.wipe()
		return Metadata{}, nil, err
	}
	return md, keys, nil
}

// newKDFParams resolves the Argon2id parameters and flags for a new store and
// generates its salt.
func newKDFParams(options *Options) (salt []byte, time, memory uint32, threads uint8, flags uint16, err error) {
	time, memory, threads, flags, err = options.creationParams()
	if err != nil {
		return nil, 0, 0, 0, 0, err
	}
	salt, err = newSalt()
	if err != nil {
		return nil, 0, 0, 0, 0, err
	}
	return salt, time, memory, threads, flags, nil
}

// encryptionFlags resolves the encryption selection for a new store.
func encryptionFlags(options *Options) (uint16, error) {
	if options.Plaintext {
		if options.Encrypt != 0 {
			return 0, fmt.Errorf("%w: Plaintext conflicts with encryption flags", ErrInvalidOptions)
		}
		return EncryptNone, nil
	}

	flags := options.Encrypt
	if flags == 0 {
		flags = EncryptAll
	}
	return flags, nil
}

// canaryKeychain builds the one-key keychain used for the canary checks.
func canaryKeychain(key []byte) (*crypto.Keychain, error) {
	return crypto.NewKeychain([][]byte{key})
}

// canaryPlaintextFor returns the canary plaintext bound to flags. The write
// access attacker model this protects against is simple: without the binding, a
// flipped encryption flag still verifies, so Open would silently succeed in a
// different encryption mode. Binding the flags into the authenticated payload
// turns that into an authentication failure.
func canaryPlaintextFor(flags uint16) string {
	return fmt.Sprintf("%s/0x%04x", canaryPlaintext, flags)
}

// sealCanary seals the flag-bound canary plaintext with the canary key.
func sealCanary(key []byte, flags uint16) ([]byte, error) {
	keychain, err := canaryKeychain(key)
	if err != nil {
		return nil, fmt.Errorf("harbor: initialize canary key: %w", err)
	}
	sealed, err := keychain.Seal([]byte(canaryPlaintextFor(flags)))
	if err != nil {
		return nil, fmt.Errorf("harbor: seal canary: %w", err)
	}
	return sealed, nil
}

// verifyCanary proves that key decrypts the stored canary and that the
// persisted flags are the ones it was sealed with.
func verifyCanary(key, sealed []byte, flags uint16) error {
	keychain, err := canaryKeychain(key)
	if err != nil {
		return err
	}
	return crypto.VerifyCanary(keychain, sealed, canaryPlaintextFor(flags))
}

// deriveKeys stretches the password and derives one key per component.
//
// The pipeline is: Argon2id(password) -> 32-byte seed -> SLIP-0021 master ->
// one labelled child per component. SLIP-0021 gives each child an independent
// HMAC-SHA512 key, so compromise of one sub-store key does not expose the
// others. The returned keys are fresh slices owned by the caller.
func deriveKeys(password string, salt []byte, time, memory uint32, threads uint8) (*storeKeys, error) {
	passwordBytes := []byte(password)
	defer wipe(passwordBytes)

	seed := deriveSeed(passwordBytes, salt, time, memory, threads)
	defer wipe(seed)

	master, err := slip21.New(seed)
	if err != nil {
		return nil, fmt.Errorf("harbor: derive master key: %w", err)
	}
	defer master.Destroy()

	derive := func(label string) ([]byte, error) {
		node, err := master.Derive([]byte(label))
		if err != nil {
			return nil, fmt.Errorf("harbor: derive %s key: %w", label, err)
		}
		defer node.Destroy()
		key := node.Key()
		if len(key) != KeySize {
			return nil, fmt.Errorf("harbor: derived %s key has %d bytes", label, len(key))
		}
		return key, nil
	}

	keys := &storeKeys{}
	labels := []struct {
		label string
		dst   *[]byte
	}{
		{canaryPlaintext, &keys.canary},
		{labelDB, &keys.record},
		{labelFS, &keys.file},
		{labelTS, &keys.series},
		{labelVec, &keys.vector},
		{labelFTS, &keys.search},
	}
	for _, item := range labels {
		*item.dst, err = derive(item.label)
		if err != nil {
			keys.wipe()
			return nil, err
		}
	}

	// Derive the audit Ed25519 signing keys from the same seed, one per store,
	// mirroring the per-store SLIP-21 keys. SLIP-10 derives via hardened
	// integer indices, so the store order is fixed by the auditPath constants.
	auditPaths := []struct {
		path string
		dst  *ed25519.PrivateKey
	}{
		{auditPathDB, &keys.auditDB},
		{auditPathFS, &keys.auditFS},
		{auditPathTS, &keys.auditTS},
		{auditPathVec, &keys.auditVec},
	}
	for _, item := range auditPaths {
		_, priv, err := audit.DeriveKey(seed, item.path)
		if err != nil {
			keys.wipe()
			return nil, err
		}
		*item.dst = priv
	}
	return keys, nil
}

// wipe overwrites secret bytes. KeepAlive keeps the compiler from treating the
// loop as dead once b is no longer otherwise referenced.
func wipe(b []byte) {
	for i := range b {
		b[i] = 0
	}
	runtime.KeepAlive(b)
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
func (s *Store) File(path ...string) *file.Store {
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
func (s *Store) Audit() *audit.Recorder {
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
func (s *Store) VerifyAudit() (audit.Result, error) {
	if s == nil {
		return audit.Result{}, ErrClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil {
		return audit.Result{}, ErrClosed
	}
	keys := make(map[string]ed25519.PublicKey, 4)
	if hasFlag(s.auditFlags, AuditDB) {
		keys[audit.StoreDB] = s.auditPubs.auditDB
	}
	if hasFlag(s.auditFlags, AuditFS) {
		keys[audit.StoreFS] = s.auditPubs.auditFS
	}
	if hasFlag(s.auditFlags, AuditTS) {
		keys[audit.StoreTS] = s.auditPubs.auditTS
	}
	if hasFlag(s.auditFlags, AuditVec) {
		keys[audit.StoreVec] = s.auditPubs.auditVec
	}
	return audit.Verify(s.bs, keys)
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
	if flushErr != nil && closeErr != nil {
		return errors.Join(flushErr, closeErr)
	}
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}
