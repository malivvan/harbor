package db

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"os"

	"github.com/malivvan/storm/internal/encryption"
	bolt "go.etcd.io/bbolt"
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
// encryption feature enabled, keeps the plaintext on-disk format: the existing
// Open constructor and every store opened without encryption behave exactly as
// before.
type Config struct {
	// Cipher selects the AEAD cipher. CipherAuto (the default) resolves to
	// AES-256-GCM.
	Cipher CipherSuite

	// Key is a raw 32-byte master key. Exactly one of Key and KeyProvider must
	// be set when encryption is enabled.
	Key []byte

	// KeyProvider supplies the master key instead of Key.
	KeyProvider KeyProvider

	// EncryptValues seals record values and typed key/value values. The codec
	// name and bucket names stay readable.
	EncryptValues bool

	// EncryptMetadata seals auto-increment counters.
	EncryptMetadata bool

	// EncryptKeys deterministically encrypts primary keys, index values and
	// key/value keys with AES-SIV, so exact lookups keep working while the
	// stored keys no longer reveal the indexed data. Ordered and prefix
	// lookups that rely on index ordering return ErrEncryptedOrdering.
	EncryptKeys bool
}

func (c *Config) encryptionEnabled() bool {
	return c != nil && (c.EncryptValues || c.EncryptMetadata || c.EncryptKeys)
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
// new writes, and every key is tried when decrypting existing data. Store
// rotation (Store.ReEncrypt, Store.RotateAll) adopts a single new primary key.
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

// Key domains. Every deterministic key encryption is domain-separated, so the
// same logical value yields a different physical key in each context.
const (
	keyDomainID    = "id"
	keyDomainIndex = "index"
	keyDomainKV    = "kv"
)

const (
	sivKeySize    = 64
	sivInfoPrefix = "storm/db/keys/"
)

// encState is the shared encryption configuration of a Store. Reachable from
// every node through its Store pointer. A nil encState means plaintext: every
// helper is then an exact passthrough to the codec and toBytes.
type encState struct {
	// engine is the primary engine used for new writes.
	engine CipherEngine

	// engines holds the primary engine first, then any rotation fallbacks.
	engines []CipherEngine

	// cipher is the resolved cipher suite.
	cipher CipherSuite

	// keys holds the raw master keys, primary first. They are kept because key
	// encryption derives per-domain SIV keys (and rotation re-derives them).
	keys [][]byte

	encValues   bool
	encMetadata bool
	encKeys     bool

	// sivs holds the per-domain deterministic engines, derived from the
	// primary key. Read-only after construction.
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

// sealValues seals a marshaled value when EncryptValues is enabled.
func (e *encState) sealValues(plain []byte) ([]byte, error) {
	if e == nil || !e.encValues || len(plain) == 0 {
		return plain, nil
	}
	return e.seal(plain)
}

// openValues reverses sealValues.
func (e *encState) openValues(raw []byte) ([]byte, error) {
	if e == nil || !e.encValues || len(raw) == 0 {
		return raw, nil
	}
	return e.open(raw)
}

// sealMetadata seals a metadata blob when EncryptMetadata is enabled.
func (e *encState) sealMetadata(plain []byte) ([]byte, error) {
	if e == nil || !e.encMetadata {
		return plain, nil
	}
	return e.seal(plain)
}

// openMetadata reverses sealMetadata.
func (e *encState) openMetadata(raw []byte) ([]byte, error) {
	if e == nil || !e.encMetadata {
		return raw, nil
	}
	return e.open(raw)
}

// sealKey deterministically encrypts a key component for the given domain when
// EncryptKeys is enabled. The output is base64url encoded so encrypted keys
// stay safe to use as bbolt keys and to concatenate with index separators.
func (e *encState) sealKey(plain []byte, domain string) ([]byte, error) {
	if e == nil || !e.encKeys || len(plain) == 0 {
		return plain, nil
	}
	engine, ok := e.sivs[domain]
	if !ok {
		return nil, fmt.Errorf("%w: unknown key domain %q", ErrInvalidConfig, domain)
	}
	sealed, err := engine.Encrypt(plain)
	if err != nil {
		return nil, err
	}
	out := make([]byte, base64.RawURLEncoding.EncodedLen(len(sealed)))
	base64.RawURLEncoding.Encode(out, sealed)
	return out, nil
}

// openKey reverses sealKey. It is only needed to re-key a store during
// rotation: normal lookups encrypt the plaintext value they are searching for.
func (e *encState) openKey(cipherKey []byte, domain string) ([]byte, error) {
	if e == nil || !e.encKeys || len(cipherKey) == 0 {
		return cipherKey, nil
	}
	engine, ok := e.sivs[domain]
	if !ok {
		return nil, fmt.Errorf("%w: unknown key domain %q", ErrInvalidConfig, domain)
	}
	sealed := make([]byte, base64.RawURLEncoding.DecodedLen(len(cipherKey)))
	n, err := base64.RawURLEncoding.Decode(sealed, cipherKey)
	if err != nil {
		return nil, fmt.Errorf("%w: malformed encrypted key: %v", ErrAuthFailed, err)
	}
	return engine.Decrypt(sealed[:n])
}

// Marker constants. The marker is stored unencrypted in the __database bucket
// so a plaintext open can detect an encrypted store without a key.
const (
	markerKey       = "\x00__db_enc__"
	markerMagic     = "DENC"
	markerVersion   = byte(1)
	canaryPlaintext = "storm/db/aead/v1"
)

const (
	markerFlagValues   = 1 << 0
	markerFlagMetadata = 1 << 1
	markerFlagKeys     = 1 << 2
)

// marker describes the encryption mode recorded in a store.
type marker struct {
	cipher      CipherSuite
	encValues   bool
	encMetadata bool
	encKeys     bool
}

func (m marker) matches(e *encState) bool {
	return m.cipher == e.cipher &&
		m.encValues == e.encValues &&
		m.encMetadata == e.encMetadata &&
		m.encKeys == e.encKeys
}

// encodeMarker serializes the marker with a sealed canary used to verify the
// key: opening the canary with the wrong key fails with ErrAuthFailed.
func encodeMarker(e *encState) ([]byte, error) {
	canary, err := e.seal([]byte(canaryPlaintext))
	if err != nil {
		return nil, err
	}

	var flags byte
	if e.encValues {
		flags |= markerFlagValues
	}
	if e.encMetadata {
		flags |= markerFlagMetadata
	}
	if e.encKeys {
		flags |= markerFlagKeys
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
		cipher:      CipherSuite(raw[5]),
		encValues:   raw[6]&markerFlagValues != 0,
		encMetadata: raw[6]&markerFlagMetadata != 0,
		encKeys:     raw[6]&markerFlagKeys != 0,
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

// loadMarker reads the store marker, if present, in its own read transaction.
func (n *node) loadMarker(ctx context.Context) (marker, []byte, bool, error) {
	var (
		m      marker
		canary []byte
		found  bool
	)
	err := n.s.bolt.View(func(tx *bolt.Tx) error {
		b := n.getBucket(tx, databaseInfo)
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

// storeMarker writes the store marker in its own write transaction.
func (n *node) storeMarker(ctx context.Context, e *encState) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	raw, err := encodeMarker(e)
	if err != nil {
		return err
	}
	return n.s.bolt.Update(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}
		b, err := n.createBucketIfNotExists(tx, databaseInfo)
		if err != nil {
			return err
		}
		if err := b.Put([]byte(markerKey), raw); err != nil {
			return err
		}
		// The version record written by an earlier plaintext open is metadata,
		// not user data: drop it so checkVersion rewrites it under the new
		// encryption settings (sealed value, encrypted key).
		if err := b.Delete([]byte(versionKey)); err != nil {
			return err
		}
		return checkContext(ctx)
	})
}

// hasUserData reports whether the store already holds data written by an
// earlier open: any bucket other than __database, any plain key at the root
// level, or any key inside __database (the version record included).
func (n *node) hasUserData(ctx context.Context) (bool, error) {
	var has bool
	err := n.s.bolt.View(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		c := n.cursor(tx)
		if c == nil {
			return nil
		}

		for k, v := c.First(); k != nil; k, v = c.Next() {
			if err := checkContext(ctx); err != nil {
				return err
			}
			if v != nil {
				has = true
				return nil
			}
			if string(k) != databaseInfo {
				has = true
				return nil
			}
		}

		b := n.getBucket(tx, databaseInfo)
		if b == nil {
			return nil
		}
		// The reserved entries of __database are written by Storm itself:
		// the marker, the version record and the per-bucket metadata bucket.
		// Anything else counts as user data.
		if err := b.ForEach(func(key, value []byte) error {
			if string(key) == markerKey || string(key) == versionKey {
				return nil
			}
			if value == nil && string(key) == metadataBucket {
				return nil
			}
			has = true
			return nil
		}); err != nil {
			return err
		}
		return checkContext(ctx)
	})
	if err != nil {
		return false, err
	}
	return has, nil
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
		cipher:      cipherSuite,
		keys:        keys,
		encValues:   cfg.EncryptValues,
		encMetadata: cfg.EncryptMetadata,
		encKeys:     cfg.EncryptKeys,
		engines:     make([]CipherEngine, 0, len(keys)),
	}

	for _, key := range keys {
		engine, err := encryption.NewCipherEngine(cipherSuite, key)
		if err != nil {
			return nil, err
		}
		state.engines = append(state.engines, engine)
	}
	state.engine = state.engines[0]

	if state.encKeys {
		// Deterministic key encryption is derived from a single master key:
		// a fallback key could decrypt values but not reproduce the physical
		// keys, so lookups would silently miss. Rotate instead.
		if len(keys) > 1 {
			return nil, fmt.Errorf("%w: multiple keys cannot be combined with EncryptKeys; use Store.ReEncrypt to rotate", ErrInvalidConfig)
		}
		state.sivs = make(map[string]*encryption.SIVEngine, 3)
		for _, domain := range []string{keyDomainID, keyDomainIndex, keyDomainKV} {
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
		return nil, fmt.Errorf("db: deriving SIV key for %q: %w", domain, err)
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

// encFromNode returns the encryption state of the Store that owns n, or nil
// for a plaintext store.
func encFromNode(n Node) *encState {
	nd, ok := n.(*node)
	if !ok || nd.s == nil {
		return nil
	}
	return nd.s.enc
}

// nodeEnc returns the encryption state of the node, or nil for a plaintext
// store.
func (n *node) nodeEnc() *encState {
	if n == nil || n.s == nil {
		return nil
	}
	return n.s.enc
}

// seal encrypts p when the store is encrypted, and returns p untouched
// otherwise.
func (n *node) seal(p []byte) ([]byte, error) {
	return n.nodeEnc().sealValues(p)
}

// open reverses seal.
func (n *node) open(c []byte) ([]byte, error) {
	return n.nodeEnc().openValues(c)
}

// marshal encodes v with the node codec and seals the result when the store
// encrypts values. With no encryption configured it is an exact passthrough to
// codec.Marshal.
func (n *node) marshal(v any) ([]byte, error) {
	raw, err := n.codec.Marshal(v)
	if err != nil {
		return nil, err
	}
	return n.seal(raw)
}

// unmarshal opens raw and decodes it with the node codec. With no encryption
// configured it is an exact passthrough to codec.Unmarshal.
func (n *node) unmarshal(raw []byte, v any) error {
	plain, err := n.open(raw)
	if err != nil {
		return err
	}
	return n.codec.Unmarshal(plain, v)
}

// keyBytes encodes a key with toBytes and, when the store encrypts keys,
// encrypts the result deterministically for the given domain. Exact lookups
// keep working because the same logical key always maps to the same physical
// key.
func (n *node) keyBytes(k any, domain string) ([]byte, error) {
	raw, err := toBytes(k, n.codec)
	if err != nil {
		return nil, err
	}
	return n.nodeEnc().sealKey(raw, domain)
}

// fieldKeyDomain returns the key domain used by a field: primary keys and
// primary-key lookups use the id domain, everything else the index domain.
func fieldKeyDomain(field *fieldConfig) string {
	if field != nil && field.IsID {
		return keyDomainID
	}
	return keyDomainIndex
}

// errEncryptedOrdering returns ErrEncryptedOrdering when the store encrypts
// keys, because ciphertext ordering is unrelated to value ordering.
func (n *node) errEncryptedOrdering() error {
	if enc := n.nodeEnc(); enc != nil && enc.encKeys {
		return ErrEncryptedOrdering
	}
	return nil
}
