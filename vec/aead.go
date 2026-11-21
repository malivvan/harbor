package vec

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"os"

	"github.com/malivvan/storm/internal/encryption"
	"go.etcd.io/bbolt"
	"golang.org/x/crypto/hkdf"
)

// CipherSuite selects the AEAD cipher used by an encrypted store.
type CipherSuite = encryption.CipherSuite

// CipherEngine performs AEAD encryption and decryption.
type CipherEngine = encryption.CipherEngine

const (
	// CipherAuto selects AES-256-GCM.
	CipherAuto = encryption.CipherAuto
	// CipherAES256GCM uses AES-256-GCM.
	CipherAES256GCM = encryption.CipherAES256GCM
	// CipherXChaCha20Poly1305 uses XChaCha20-Poly1305.
	CipherXChaCha20Poly1305 = encryption.CipherXChaCha20Poly1305

	// KeySize is the required key length in bytes (256 bits).
	KeySize = encryption.KeySize
)

// Config enables optional encryption for a Store. A nil Config, or one with no
// encryption feature enabled, keeps the plaintext on-disk format: Open and
// every store opened without encryption behave exactly as before.
type Config struct {
	// Cipher selects the AEAD cipher. CipherAuto (the default) resolves to
	// AES-256-GCM.
	Cipher CipherSuite

	// Key is a raw 32-byte master key. Exactly one of Key and KeyProvider must
	// be set when encryption is enabled.
	Key []byte

	// KeyProvider supplies the master key instead of Key.
	KeyProvider KeyProvider

	// EncryptPoints seals point blobs (vectors, payloads and quantized vectors)
	// and collection metadata.
	EncryptPoints bool

	// EncryptPointIDs deterministically encrypts point IDs with AES-SIV, so
	// exact lookups and deletes keep working while the stored keys no longer
	// reveal them.
	EncryptPointIDs bool

	// EncryptCollectionNames deterministically encrypts collection bucket and
	// metadata names with AES-SIV. Listing still returns the original names.
	EncryptCollectionNames bool
}

func (c *Config) encryptionEnabled() bool {
	return c != nil && (c.EncryptPoints || c.EncryptPointIDs || c.EncryptCollectionNames)
}

// KeyProvider supplies the raw 32-byte master key used for encryption.
type KeyProvider interface {
	Key() ([]byte, error)
}

// RawKeyProvider supplies a fixed 32-byte master key.
type RawKeyProvider struct {
	key []byte
}

// NewRawKeyProvider returns a provider that uses key directly.
func NewRawKeyProvider(key []byte) (*RawKeyProvider, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, KeySize, len(key))
	}
	return &RawKeyProvider{key: append([]byte(nil), key...)}, nil
}

// Key returns a copy of the raw master key.
func (p *RawKeyProvider) Key() ([]byte, error) {
	return append([]byte(nil), p.key...), nil
}

// EnvKeyProvider reads a raw 32-byte master key from an environment variable.
type EnvKeyProvider struct {
	envVar string
}

// NewEnvKeyProvider creates a key provider backed by an environment variable.
func NewEnvKeyProvider(envVar string) *EnvKeyProvider {
	return &EnvKeyProvider{envVar: envVar}
}

// Key returns the key stored in the configured environment variable.
func (e *EnvKeyProvider) Key() ([]byte, error) {
	key := []byte(os.Getenv(e.envVar))
	if len(key) == 0 {
		return nil, fmt.Errorf("%w: environment variable %s not set", ErrInvalidKey, e.envVar)
	}
	if len(key) != KeySize {
		return nil, fmt.Errorf("%w: key from environment variable must be %d bytes, got %d", ErrInvalidKey, KeySize, len(key))
	}
	return key, nil
}

// MultiKeyProvider supplies several keys for rotation: the first is used for
// new writes, and every key is tried when decrypting existing data.
type MultiKeyProvider struct {
	providers []KeyProvider
}

// NewMultiKeyProvider creates a multi-key provider. At least one provider is
// required; the first one is the primary key.
func NewMultiKeyProvider(providers ...KeyProvider) (*MultiKeyProvider, error) {
	if len(providers) == 0 {
		return nil, fmt.Errorf("%w: at least one key provider required", ErrInvalidConfig)
	}
	return &MultiKeyProvider{providers: providers}, nil
}

// Key returns the primary key.
func (m *MultiKeyProvider) Key() ([]byte, error) {
	return m.providers[0].Key()
}

// TryKeys returns every configured key, primary first.
func (m *MultiKeyProvider) TryKeys() ([][]byte, error) {
	keys := make([][]byte, 0, len(m.providers))
	for _, provider := range m.providers {
		key, err := provider.Key()
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// Key domains. Deterministic key encryption is domain-separated, so the same
// logical value yields a different physical name in each context.
const (
	keyDomainID   = "id"
	keyDomainName = "name"
)

const (
	sivKeySize    = 64
	sivInfoPrefix = "storm/vec/keys/"
)

// encState is the shared encryption configuration of a Store. A nil encState
// means plaintext: every helper is then an exact passthrough to the binco,
// JSON and raw-key paths used today.
type encState struct {
	// engine is the primary engine used for new writes.
	engine CipherEngine

	// engines holds the primary engine first, then any rotation fallbacks.
	engines []CipherEngine

	// cipher is the resolved cipher suite.
	cipher CipherSuite

	// keys holds the raw master keys, primary first. Key names are derived from
	// them, and rotation re-derives them.
	keys [][]byte

	encPoints bool
	encIDs    bool
	encNames  bool

	// sivs holds the per-domain deterministic engines derived from the primary
	// key. Read-only after construction.
	sivs map[string]*encryption.SIVEngine
}

// enabled reports whether the state encrypts anything.
func (e *encState) enabled() bool {
	return e != nil
}

// seal encrypts plain with the primary engine, prefixing a fresh random nonce.
func (e *encState) seal(plain []byte) ([]byte, error) {
	nonce, err := encryption.GenerateNonce(e.cipher)
	if err != nil {
		return nil, err
	}
	sealed, err := e.engine.Encrypt(nonce, plain)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(nonce)+len(sealed))
	out = append(out, nonce...)
	return append(out, sealed...), nil
}

// open decrypts raw with the primary engine and any rotation fallbacks.
func (e *encState) open(raw []byte) ([]byte, error) {
	if e == nil {
		return raw, nil
	}
	for _, engine := range e.engines {
		nonceSize := engine.NonceSize()
		if len(raw) < nonceSize+engine.Overhead() {
			continue
		}
		plain, err := engine.Decrypt(raw[:nonceSize], raw[nonceSize:])
		if err == nil {
			return plain, nil
		}
	}
	return nil, ErrAuthFailed
}

// sealPoints seals a point blob or collection metadata when EncryptPoints is
// enabled.
func (e *encState) sealPoints(plain []byte) ([]byte, error) {
	if e == nil || !e.encPoints || len(plain) == 0 {
		return plain, nil
	}
	return e.seal(plain)
}

// openPoints reverses sealPoints.
func (e *encState) openPoints(raw []byte) ([]byte, error) {
	if e == nil || !e.encPoints || len(raw) == 0 {
		return raw, nil
	}
	return e.open(raw)
}

// sealName deterministically encrypts a point ID or collection name for the
// given domain, base64url encoded so encrypted names stay usable as bbolt keys.
func (e *encState) sealName(plain string, domain string) ([]byte, error) {
	if e == nil {
		return []byte(plain), nil
	}
	if (domain == keyDomainID && !e.encIDs) || (domain == keyDomainName && !e.encNames) {
		return []byte(plain), nil
	}
	engine, ok := e.sivs[domain]
	if !ok {
		return nil, fmt.Errorf("%w: unknown key domain %q", ErrInvalidConfig, domain)
	}
	sealed, err := engine.Encrypt([]byte(plain))
	if err != nil {
		return nil, err
	}
	out := make([]byte, base64.RawURLEncoding.EncodedLen(len(sealed)))
	base64.RawURLEncoding.Encode(out, sealed)
	return out, nil
}

// openName reverses sealName.
func (e *encState) openName(cipherName []byte, domain string) (string, error) {
	if e == nil {
		return string(cipherName), nil
	}
	if (domain == keyDomainID && !e.encIDs) || (domain == keyDomainName && !e.encNames) {
		return string(cipherName), nil
	}
	engine, ok := e.sivs[domain]
	if !ok {
		return "", fmt.Errorf("%w: unknown key domain %q", ErrInvalidConfig, domain)
	}
	sealed := make([]byte, base64.RawURLEncoding.DecodedLen(len(cipherName)))
	n, err := base64.RawURLEncoding.Decode(sealed, cipherName)
	if err != nil {
		return "", fmt.Errorf("%w: malformed encrypted name: %v", ErrAuthFailed, err)
	}
	plain, err := engine.Decrypt(sealed[:n])
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

// Marker constants. The marker is stored unencrypted in the collections
// metadata bucket so a plaintext open can detect an encrypted store without a
// key.
const (
	markerKey       = "\x00__vec_enc__"
	markerMagic     = "VENC"
	markerVersion   = byte(1)
	canaryPlaintext = "storm/vec/aead/v1"
)

const (
	markerFlagPoints = 1 << 0
	markerFlagIDs    = 1 << 1
	markerFlagNames  = 1 << 2
)

// marker describes the encryption mode recorded in a store.
type marker struct {
	cipher    CipherSuite
	encPoints bool
	encIDs    bool
	encNames  bool
}

func (m marker) matches(e *encState) bool {
	return m.cipher == e.cipher &&
		m.encPoints == e.encPoints &&
		m.encIDs == e.encIDs &&
		m.encNames == e.encNames
}

// encodeMarker serializes the marker with a sealed canary used to verify the
// key: opening the canary with the wrong key fails with ErrAuthFailed.
func encodeMarker(e *encState) ([]byte, error) {
	canary, err := e.seal([]byte(canaryPlaintext))
	if err != nil {
		return nil, err
	}

	var flags byte
	if e.encPoints {
		flags |= markerFlagPoints
	}
	if e.encIDs {
		flags |= markerFlagIDs
	}
	if e.encNames {
		flags |= markerFlagNames
	}

	raw := make([]byte, 0, 4+1+1+1+len(canary))
	raw = append(raw, markerMagic...)
	raw = append(raw, markerVersion, byte(e.cipher), flags)
	raw = append(raw, canary...)
	return raw, nil
}

// decodeMarker parses a marker and returns its sealed canary.
func decodeMarker(raw []byte) (marker, []byte, error) {
	const fixedSize = 4 + 1 + 1 + 1
	if len(raw) < fixedSize {
		return marker{}, nil, fmt.Errorf("%w: corrupt encryption marker", ErrWrongMode)
	}
	if string(raw[:4]) != markerMagic {
		return marker{}, nil, fmt.Errorf("%w: corrupt encryption marker", ErrWrongMode)
	}
	if raw[4] != markerVersion {
		return marker{}, nil, fmt.Errorf("%w: unsupported encryption marker version", ErrWrongMode)
	}

	m := marker{
		cipher:    CipherSuite(raw[5]),
		encPoints: raw[6]&markerFlagPoints != 0,
		encIDs:    raw[6]&markerFlagIDs != 0,
		encNames:  raw[6]&markerFlagNames != 0,
	}
	return m, raw[fixedSize:], nil
}

// verifyCanary confirms the configured keys decrypt the stored canary.
func (e *encState) verifyCanary(canary []byte) error {
	plain, err := e.open(canary)
	if err != nil {
		return err
	}
	if string(plain) != canaryPlaintext {
		return ErrAuthFailed
	}
	return nil
}

// loadMarker reads the store marker, if present.
func (s *Store) loadMarker() (marker, []byte, bool, error) {
	var (
		m      marker
		canary []byte
		found  bool
	)
	err := s.db.View(func(tx *bbolt.Tx) error {
		b, err := s.metaBucket(tx, false)
		if err != nil {
			return err
		}
		if b == nil {
			return nil
		}
		raw := b.Get([]byte(markerKey))
		if raw == nil {
			return nil
		}
		var decodeErr error
		m, canary, decodeErr = decodeMarker(raw)
		found = true
		return decodeErr
	})
	if err != nil {
		return marker{}, nil, false, err
	}
	return m, canary, found, nil
}

// storeMarker writes the store marker.
func (s *Store) storeMarker(e *encState) error {
	raw, err := encodeMarker(e)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b, err := s.metaBucket(tx, true)
		if err != nil {
			return err
		}
		return b.Put([]byte(markerKey), raw)
	})
}

// hasData reports whether the store already holds collections, points or
// collection metadata written by an earlier open.
func (s *Store) hasData() (bool, error) {
	var has bool
	err := s.db.View(func(tx *bbolt.Tx) error {
		if len(s.root) > 0 {
			root := tx.Bucket(s.root)
			if root == nil {
				return nil
			}
			return root.ForEach(func(name, value []byte) error {
				if string(name) != collectionsMetaBucket {
					has = true
				}
				return nil
			})
		}

		return tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
			if string(name) != collectionsMetaBucket {
				has = true
			}
			return nil
		})
	})
	if err != nil {
		return false, err
	}
	if has {
		return true, nil
	}

	meta, err := s.metaBucketForRead()
	if err != nil {
		return false, err
	}
	for _, name := range meta {
		if name != markerKey {
			return true, nil
		}
	}
	return false, nil
}

// metaBucketForRead returns the raw keys of the metadata bucket.
func (s *Store) metaBucketForRead() ([]string, error) {
	var keys []string
	err := s.db.View(func(tx *bbolt.Tx) error {
		b, err := s.metaBucket(tx, false)
		if err != nil {
			return err
		}
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, _ []byte) error {
			keys = append(keys, string(k))
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// resolveConfig turns a Config into an encryption state. It returns nil for a
// nil Config and for a Config that enables no encryption feature.
func resolveConfig(cfg *Config) (*encState, error) {
	if !cfg.encryptionEnabled() {
		return nil, nil
	}

	cipherSuite, err := encryption.Resolve(cfg.Cipher)
	if err != nil {
		return nil, ErrUnsupportedCipher
	}

	keys, err := resolveKeys(cfg)
	if err != nil {
		return nil, err
	}

	state := &encState{
		cipher:    cipherSuite,
		keys:      keys,
		encPoints: cfg.EncryptPoints,
		encIDs:    cfg.EncryptPointIDs,
		encNames:  cfg.EncryptCollectionNames,
		engines:   make([]CipherEngine, 0, len(keys)),
	}

	for _, key := range keys {
		engine, err := encryption.NewCipherEngine(cipherSuite, key)
		if err != nil {
			return nil, err
		}
		state.engines = append(state.engines, engine)
	}
	state.engine = state.engines[0]

	if state.encIDs || state.encNames {
		// Deterministic name encryption is derived from a single master key: a
		// fallback key could decrypt values but not reproduce the physical
		// names, so lookups would silently miss. Rotate instead.
		if len(keys) > 1 {
			return nil, fmt.Errorf("%w: multiple keys cannot be combined with encrypted names; use ReEncryptCollection or RotateAll", ErrInvalidConfig)
		}
		state.sivs = make(map[string]*encryption.SIVEngine, 2)
		for _, domain := range []string{keyDomainID, keyDomainName} {
			engine, err := deriveSIV(keys[0], domain)
			if err != nil {
				return nil, err
			}
			state.sivs[domain] = engine
		}
	}

	return state, nil
}

// deriveSIV derives a 64-byte AES-SIV key for one domain with HKDF-SHA256.
func deriveSIV(masterKey []byte, domain string) (*encryption.SIVEngine, error) {
	if len(masterKey) != KeySize {
		return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, KeySize, len(masterKey))
	}

	key := make([]byte, sivKeySize)
	info := []byte(sivInfoPrefix + domain + "/v1")
	if _, err := io.ReadFull(hkdf.New(sha256.New, masterKey, nil, info), key); err != nil {
		return nil, fmt.Errorf("vec: deriving SIV key for %q: %w", domain, err)
	}
	return encryption.NewSIVEngine(key)
}

// resolveKeys returns the master keys, primary first.
func resolveKeys(cfg *Config) ([][]byte, error) {
	if cfg.Key != nil && cfg.KeyProvider != nil {
		return nil, fmt.Errorf("%w: set either Key or KeyProvider, not both", ErrInvalidConfig)
	}

	if cfg.KeyProvider == nil {
		if len(cfg.Key) != KeySize {
			return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, KeySize, len(cfg.Key))
		}
		return [][]byte{append([]byte(nil), cfg.Key...)}, nil
	}

	providers := []KeyProvider{cfg.KeyProvider}
	if multi, ok := cfg.KeyProvider.(*MultiKeyProvider); ok {
		providers = multi.providers
	}

	keys := make([][]byte, 0, len(providers))
	for _, provider := range providers {
		key, err := provider.Key()
		if err != nil {
			return nil, err
		}
		if len(key) != KeySize {
			return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, KeySize, len(key))
		}
		keys = append(keys, append([]byte(nil), key...))
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("%w: no key provider", ErrInvalidConfig)
	}
	return keys, nil
}

// sealBytes seals a point blob or metadata payload. With no encryption
// configured it returns p untouched.
func (s *Store) sealBytes(p []byte) ([]byte, error) {
	return s.enc.sealPoints(p)
}

// openBytes reverses sealBytes.
func (s *Store) openBytes(c []byte) ([]byte, error) {
	return s.enc.openPoints(c)
}

// keyBytes returns the stored key for a point ID: the SIV-encrypted ID when
// point IDs are encrypted, the raw ID otherwise.
//
// AES-SIV encryption cannot fail with a valid key, so a nil result is
// unreachable; callers that write guard against it anyway.
func (s *Store) keyBytes(id string) []byte {
	out, err := s.enc.sealName(id, keyDomainID)
	if err != nil {
		return nil
	}
	return out
}

// bucketKey returns the stored name for a collection: the SIV-encrypted name
// when collection names are encrypted, the raw name otherwise.
func (s *Store) bucketKey(name string) []byte {
	out, err := s.enc.sealName(name, keyDomainName)
	if err != nil {
		return nil
	}
	return out
}

// logicalName turns a stored collection name back into the name the caller
// knows.
func (s *Store) logicalName(physical []byte) (string, error) {
	return s.enc.openName(physical, keyDomainName)
}
