// Package storm exposes a single bbolt-backed store that combines the db, fs,
// ts and vec sub-stores behind one password.
//
// The password is stretched with Argon2id using the salt and cost parameters
// kept in an unencrypted "storm.Metadata" record, then one key is derived per
// sub-store. The password itself is never stored. A sealed canary in the
// Metadata detects a wrong password even for stores created with encryption
// disabled.
//
// The key/value API (Get, Set, Delete, Has, Keys, Len) uses its own bucket, so
// user keys never collide with store Metadata. When the store was created with
// EncryptKVValues (included in EncryptAll, the default), values are sealed with
// a dedicated AES-256-GCM key; keys stay in the clear so byte-ordered listing
// keeps working. Use EncryptNone/Plaintext to opt out.
package storm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"

	"github.com/malivvan/storm/db"
	"github.com/malivvan/storm/fs"
	"github.com/malivvan/storm/internal/crypto"
	"github.com/malivvan/storm/internal/slip21"
	"github.com/malivvan/storm/ts"
	"github.com/malivvan/storm/vec"
	bolt "go.etcd.io/bbolt"
)

// The key/value API uses bucket 0. Store Metadata lives in its own
// "storm.Metadata" bucket (see meta.go), so user keys can never collide with it.
var kvBucket = []byte{0}

// namedRoot returns the default one-element bucket path for a sub-store.
func namedRoot(name string) [][]byte {
	return [][]byte{[]byte(name)}
}

// Domain-separated labels used to derive one key per component.
const (
	labelDB  = "storm/db/v1"
	labelFS  = "storm/fs/v1"
	labelTS  = "storm/ts/v1"
	labelVec = "storm/vec/v1"
	labelKV  = "storm/kv/v1"
)

// Errors returned by the package.
var (
	// ErrKeyNotFound is returned by Get for a missing key.
	ErrKeyNotFound = errors.New("storm: key not found")

	// ErrInvalidKey is returned by Set, Delete and Has for an empty key.
	ErrInvalidKey = errors.New("storm: empty key")

	// ErrClosed is returned by operations on a closed Store.
	ErrClosed = errors.New("storm: store is closed")

	// ErrInvalidOptions reports a nil Options value or an unusable
	// configuration.
	ErrInvalidOptions = errors.New("storm: invalid options")

	// ErrInvalidPassword reports an empty password or a password that does not
	// decrypt the store canary.
	ErrInvalidPassword = errors.New("storm: invalid password")
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
	Codec db.Codec

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

	// UseQuant enables vector quantization in the vec sub-store.
	UseQuant bool

	// Quantizer compresses vectors when UseQuant is enabled. A nil Quantizer
	// selects the default SQ8 quantizer.
	Quantizer vec.Quantizer
}

// Store is a single bbolt database that hosts the db, fs, ts and vec stores.
// Each sub-store uses its own domain-separated key derived from Password.
type Store struct {
	mu  sync.RWMutex
	bs  *bolt.DB
	db  *db.Store
	fs  *fs.Store
	ts  *ts.Store
	vec *vec.Store

	// kvc seals key/value payloads when EncryptKVValues is set. A nil keychain
	// leaves the bucket in plaintext.
	kvc *crypto.Keychain
}

// storeKeys holds the per-component keys derived for one Open call. They are
// wiped as soon as the sub-stores have copied them.
type storeKeys struct {
	canary []byte
	db     []byte
	fs     []byte
	ts     []byte
	vec    []byte
	kv     []byte
}

func (k *storeKeys) wipe() {
	if k == nil {
		return
	}
	for _, b := range [][]byte{k.canary, k.db, k.fs, k.ts, k.vec, k.kv} {
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

	// Bind each sub-store to its own derived key and explicit bucket root.
	// The persisted flags decide which features are encrypted, so the on-disk
	// mode cannot drift between runs.
	if s.db, err = db.Open(ctx, bdb, db.Config{
		Root:            namedRoot(db.Name),
		Codec:           options.Codec,
		Secret:          keys.db,
		EncryptKeys:     hasFlag(md.Flags, EncryptDBKeys),
		EncryptValues:   hasFlag(md.Flags, EncryptDBValues),
		EncryptMetadata: hasFlag(md.Flags, EncryptDBMetadata),
	}); err != nil {
		return nil, fmt.Errorf("storm: open db store: %w", err)
	}
	if s.fs, err = fs.Open(ctx, bdb, fs.Config{
		Root:            namedRoot(fs.Name),
		Secret:          keys.fs,
		EncryptMetadata: hasFlag(md.Flags, EncryptFSMetadata),
		EncryptContent:  hasFlag(md.Flags, EncryptFSContent),
	}); err != nil {
		return nil, fmt.Errorf("storm: open fs store: %w", err)
	}
	if s.ts, err = ts.Open(ctx, bdb, ts.Config{
		Root:               namedRoot(ts.Name),
		Secret:             keys.ts,
		EncryptValues:      hasFlag(md.Flags, EncryptTSValues),
		EncryptSeriesNames: hasFlag(md.Flags, EncryptTSSeriesNames),
	}); err != nil {
		return nil, fmt.Errorf("storm: open ts store: %w", err)
	}
	if hasFlag(md.Flags, EncryptKVValues) {
		if s.kvc, err = crypto.NewKeychain([][]byte{keys.kv}); err != nil {
			return nil, fmt.Errorf("storm: open key/value store: %w", err)
		}
	}
	if s.vec, err = vec.Open(ctx, bdb, vec.Config{
		Root:                   namedRoot(vec.Name),
		Secret:                 keys.vec,
		UseQuant:               options.UseQuant,
		Quantizer:              options.Quantizer,
		EncryptPoints:          hasFlag(md.Flags, EncryptVecPoints),
		EncryptPointIDs:        hasFlag(md.Flags, EncryptVecPointIDs),
		EncryptCollectionNames: hasFlag(md.Flags, EncryptVecCollectionNames),
	}); err != nil {
		return nil, fmt.Errorf("storm: open vec store: %w", err)
	}
	// The vec store shares this handle with db, fs and ts; it must never close
	// it on its own.
	s.vec.SetOwnsDB(false)

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

// newKDFParams resolves the Argon2id parameters and flags for a new store.
func newKDFParams(options *Options) (salt []byte, time, memory uint32, threads uint8, flags uint16, err error) {
	time, memory, threads = options.KDFTime, options.KDFMemory, options.KDFThreads
	switch {
	case time == 0 && memory == 0 && threads == 0:
		time, memory, threads = DefaultKDFTime, DefaultKDFMemory, DefaultKDFThreads
	case time == 0 || memory == 0 || threads == 0:
		return nil, 0, 0, 0, 0, fmt.Errorf("%w: incomplete KDF parameters", ErrInvalidOptions)
	}

	flags, err = encryptionFlags(options)
	if err != nil {
		return nil, 0, 0, 0, 0, err
	}
	if err := validateKDFParams(time, memory, threads, flags, ErrInvalidOptions); err != nil {
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
		return nil, fmt.Errorf("storm: initialize canary key: %w", err)
	}
	sealed, err := keychain.Seal([]byte(canaryPlaintextFor(flags)))
	if err != nil {
		return nil, fmt.Errorf("storm: seal canary: %w", err)
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
		return nil, fmt.Errorf("storm: derive master key: %w", err)
	}
	defer master.Destroy()

	derive := func(label string) ([]byte, error) {
		node, err := master.Derive([]byte(label))
		if err != nil {
			return nil, fmt.Errorf("storm: derive %s key: %w", label, err)
		}
		defer node.Destroy()
		key := node.Key()
		if len(key) != KeySize {
			return nil, fmt.Errorf("storm: derived %s key has %d bytes", label, len(key))
		}
		return key, nil
	}

	keys := &storeKeys{}
	labels := []struct {
		label string
		dst   *[]byte
	}{
		{canaryPlaintext, &keys.canary},
		{labelDB, &keys.db},
		{labelFS, &keys.fs},
		{labelTS, &keys.ts},
		{labelVec, &keys.vec},
		{labelKV, &keys.kv},
	}
	for _, item := range labels {
		*item.dst, err = derive(item.label)
		if err != nil {
			keys.wipe()
			return nil, err
		}
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

// storeComponent returns one sub-store while holding the read lock, or nil
// when the store is nil or closed.
func storeComponent[T any](s *Store, component func(*Store) *T) *T {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil {
		return nil
	}
	return component(s)
}

// DB returns the db sub-store.
func (s *Store) DB() *db.Store {
	return storeComponent(s, func(s *Store) *db.Store { return s.db })
}

// FS returns the fs sub-store.
func (s *Store) FS() *fs.Store {
	return storeComponent(s, func(s *Store) *fs.Store { return s.fs })
}

// TS returns the ts sub-store.
func (s *Store) TS() *ts.Store {
	return storeComponent(s, func(s *Store) *ts.Store { return s.ts })
}

// Vec returns the vec sub-store.
func (s *Store) Vec() *vec.Store {
	return storeComponent(s, func(s *Store) *vec.Store { return s.vec })
}

// EncryptFlags returns the encryption flags persisted by the store. A closed
// store returns EncryptNone.
func (s *Store) EncryptFlags() uint16 {
	_, _, _, _, flags, err := s.Metadata()
	if err != nil {
		return EncryptNone
	}
	return flags
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

// sealValue encrypts a key/value payload when EncryptKVValues is enabled.
func (s *Store) sealValue(plain []byte) ([]byte, error) {
	if s.kvc == nil {
		return plain, nil
	}
	sealed, err := s.kvc.Seal(plain)
	if err != nil {
		return nil, fmt.Errorf("storm: seal value: %w", err)
	}
	return sealed, nil
}

// openValue reverses sealValue.
func (s *Store) openValue(key string, raw []byte) ([]byte, error) {
	if s.kvc == nil {
		return raw, nil
	}
	plain, err := s.kvc.Open(raw)
	if err != nil {
		return nil, fmt.Errorf("storm: open value %q: %w", key, err)
	}
	return plain, nil
}

// lookup returns the decoded value and presence of key. It takes the store
// read lock once and checks the closed state before the caller validates the
// key, preserving the ErrClosed-precedence contract.
func (s *Store) lookup(key string) (value string, found bool, err error) {
	if s == nil {
		return "", false, ErrClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil {
		return "", false, ErrClosed
	}
	return s.lookupLocked(key)
}

// lookupLocked is lookup for callers that already hold the read lock.
func (s *Store) lookupLocked(key string) (value string, found bool, err error) {
	err = s.bs.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(kvBucket)
		if b == nil {
			return nil
		}
		raw := b.Get([]byte(key))
		if raw == nil {
			return nil
		}
		decoded, err := s.openValue(key, raw)
		if err != nil {
			return err
		}
		value = string(decoded)
		found = true
		return nil
	})
	return value, found, err
}

// Get returns the value stored under key. An empty key returns ErrInvalidKey,
// and a nil or closed store returns ErrClosed before the key is validated.
func (s *Store) Get(key string) (string, error) {
	value, found, err := s.lookup(key)
	if err != nil {
		return "", err
	}
	if key == "" {
		return "", ErrInvalidKey
	}
	if !found {
		return "", ErrKeyNotFound
	}
	return value, nil
}

// Set stores key/value in the key/value bucket. An empty value removes the key.
func (s *Store) Set(key, val string) error {
	if s == nil {
		return ErrClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil {
		return ErrClosed
	}
	if key == "" {
		return ErrInvalidKey
	}
	return s.bs.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(kvBucket)
		if err != nil {
			return err
		}
		if val == "" {
			return b.Delete([]byte(key))
		}
		raw, err := s.sealValue([]byte(val))
		if err != nil {
			return err
		}
		return b.Put([]byte(key), raw)
	})
}

// Delete removes key. Deleting a missing key is not an error.
func (s *Store) Delete(key string) error {
	if s == nil {
		return ErrClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil {
		return ErrClosed
	}
	if key == "" {
		return ErrInvalidKey
	}
	return s.bs.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(kvBucket)
		if b == nil {
			return nil
		}
		return b.Delete([]byte(key))
	})
}

// Has reports whether key exists.
func (s *Store) Has(key string) (bool, error) {
	_, found, err := s.lookup(key)
	if err != nil {
		return false, err
	}
	if key == "" {
		return false, ErrInvalidKey
	}
	return found, nil
}

// Keys returns all keys in byte order. Nested buckets are skipped.
func (s *Store) Keys() ([]string, error) {
	if s == nil {
		return nil, ErrClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil {
		return nil, ErrClosed
	}

	var keys []string
	err := s.bs.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(kvBucket)
		if b == nil {
			return nil
		}
		keys = make([]string, 0, b.Stats().KeyN)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if v == nil {
				continue
			}
			keys = append(keys, string(k))
		}
		return nil
	})
	return keys, err
}

// Len returns the number of keys in the key/value bucket.
func (s *Store) Len() (int, error) {
	if s == nil {
		return 0, ErrClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil {
		return 0, ErrClosed
	}

	// The key/value bucket never contains nested buckets, so KeyN is an exact
	// O(1) count instead of a full cursor walk.
	n := 0
	err := s.bs.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket(kvBucket); b != nil {
			n = b.Stats().KeyN
		}
		return nil
	})
	return n, err
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
	if s.vec != nil {
		if err := s.vec.Flush(); err != nil && !errors.Is(err, vec.ErrClosed) {
			flushErr = fmt.Errorf("storm: flush vec store: %w", err)
		}
	}
	closeErr := s.bs.Close()
	s.bs = nil
	s.db = nil
	s.fs = nil
	s.ts = nil
	s.vec = nil
	s.kvc = nil
	if flushErr != nil && closeErr != nil {
		return errors.Join(flushErr, closeErr)
	}
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}
