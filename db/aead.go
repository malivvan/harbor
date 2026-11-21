package db

import (
	"context"
	"fmt"

	"github.com/malivvan/storm/internal/encryption"
	bolt "go.etcd.io/bbolt"
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
type KeyProvider = encryption.KeyProvider

// RawKeyProvider supplies a fixed 32-byte master key.
type RawKeyProvider = encryption.RawKeyProvider

// NewRawKeyProvider returns a provider that uses key directly.
func NewRawKeyProvider(key []byte) (*RawKeyProvider, error) {
	return encryption.NewRawKeyProvider(key)
}

// EnvKeyProvider reads a raw 32-byte master key from an environment variable.
type EnvKeyProvider = encryption.EnvKeyProvider

// NewEnvKeyProvider creates a key provider backed by an environment variable.
func NewEnvKeyProvider(envVar string) *EnvKeyProvider {
	return encryption.NewEnvKeyProvider(envVar)
}

// MultiKeyProvider supplies several keys for rotation: the first is used for
// new writes, and every key is tried when decrypting existing data.
type MultiKeyProvider = encryption.MultiKeyProvider

// NewMultiKeyProvider creates a multi-key provider. At least one provider is
// required; the first one is the primary key.
func NewMultiKeyProvider(providers ...KeyProvider) (*MultiKeyProvider, error) {
	return encryption.NewMultiKeyProvider(providers...)
}

// Key domains. Every deterministic key encryption is domain-separated, so the
// same logical value yields a different physical key in each context.
const (
	keyDomainID    = "id"
	keyDomainIndex = "index"
	keyDomainKV    = "kv"
)

// sivInfoPrefix domain-separates the deterministic key ciphers.
const sivInfoPrefix = "storm/db/keys/"

// encState is the shared encryption configuration of a Store. Reachable from
// every node through its Store pointer. A nil encState means plaintext: every
// helper is then an exact passthrough to the codec and toBytes.
type encState struct {
	// kc holds the cipher engines: the primary key first, then rotation
	// fallbacks. A nil keychain means plaintext.
	kc *encryption.Keychain

	encValues   bool
	encMetadata bool
	encKeys     bool

	// sivs holds the per-domain deterministic name ciphers, derived from the
	// primary key. Read-only after construction.
	sivs map[string]*encryption.SIVNameCipher
}

// enabled reports whether the state encrypts anything.
func (e *encState) enabled() bool {
	return e != nil
}

// cipher returns the resolved cipher suite of an encrypted state.
func (e *encState) cipher() CipherSuite {
	if e == nil {
		return CipherAuto
	}
	return e.kc.Cipher()
}

// seal encrypts plain with the primary engine, prefixing a fresh random nonce.
func (e *encState) seal(plain []byte) ([]byte, error) {
	if e == nil {
		return plain, nil
	}
	return e.kc.Seal(plain)
}

// open decrypts raw with the primary engine and any rotation fallbacks.
func (e *encState) open(raw []byte) ([]byte, error) {
	if e == nil {
		return raw, nil
	}
	return e.kc.Open(raw)
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
// EncryptKeys is enabled. The encoding has no '_' in its alphabet, so an
// encrypted key can never contain the "__" separator of composite index keys.
func (e *encState) sealKey(plain []byte, domain string) ([]byte, error) {
	if e == nil || !e.encKeys || len(plain) == 0 {
		return plain, nil
	}
	cipher, ok := e.sivs[domain]
	if !ok {
		return nil, fmt.Errorf("%w: unknown key domain %q", ErrInvalidConfig, domain)
	}
	return cipher.SealName(plain)
}

// openKey reverses sealKey. It is only needed to re-key a store during
// rotation: normal lookups encrypt the plaintext value they are searching for.
func (e *encState) openKey(cipherKey []byte, domain string) ([]byte, error) {
	if e == nil || !e.encKeys || len(cipherKey) == 0 {
		return cipherKey, nil
	}
	cipher, ok := e.sivs[domain]
	if !ok {
		return nil, fmt.Errorf("%w: unknown key domain %q", ErrInvalidConfig, domain)
	}
	return cipher.OpenName(cipherKey)
}

// Marker constants. The marker is stored unencrypted in the __database bucket
// so a plaintext open can detect an encrypted store without a key.
const (
	markerKey       = "\x00__db_enc__"
	markerMagic     = "DENC"
	canaryPlaintext = "storm/db/aead/v1"
)

const (
	markerFlagValues   = 1 << 0
	markerFlagMetadata = 1 << 1
	markerFlagKeys     = 1 << 2
)

// markerSpec describes the on-disk marker framing shared by every store.
var markerSpec = encryption.MarkerSpec{Magic: markerMagic}

// marker describes the encryption mode recorded in a store.
type marker struct {
	cipher      CipherSuite
	encValues   bool
	encMetadata bool
	encKeys     bool
}

func (m marker) matches(e *encState) bool {
	return m.cipher == e.cipher() &&
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

	return encryption.EncodeMarker(markerSpec, encryption.Marker{
		Cipher: e.cipher(),
		Flags:  flags,
		Canary: canary,
	})
}

// decodeMarker parses a marker and returns its sealed canary.
func decodeMarker(raw []byte) (marker, []byte, error) {
	decoded, err := encryption.DecodeMarker(markerSpec, raw)
	if err != nil {
		return marker{}, nil, err
	}

	return marker{
		cipher:      decoded.Cipher,
		encValues:   decoded.Flags&markerFlagValues != 0,
		encMetadata: decoded.Flags&markerFlagMetadata != 0,
		encKeys:     decoded.Flags&markerFlagKeys != 0,
	}, decoded.Canary, nil
}

// verifyCanary confirms the configured keys decrypt the stored canary.
func (e *encState) verifyCanary(canary []byte) error {
	return encryption.VerifyCanary(e.kc, canary, canaryPlaintext)
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

	keys, err := encryption.ResolveKeys(cfg.Key, cfg.KeyProvider)
	if err != nil {
		return nil, err
	}

	kc, err := encryption.NewKeychain(cipherSuite, keys)
	if err != nil {
		return nil, err
	}

	state := &encState{
		kc:          kc,
		encValues:   cfg.EncryptValues,
		encMetadata: cfg.EncryptMetadata,
		encKeys:     cfg.EncryptKeys,
	}

	if state.encKeys {
		// Deterministic key encryption is derived from a single master key: a
		// fallback key could decrypt values but not reproduce the physical
		// keys, so lookups would silently miss.
		if err := encryption.RequireSingleKey(keys); err != nil {
			return nil, fmt.Errorf("%w; use Store.ReEncrypt to rotate keys", err)
		}
		state.sivs = make(map[string]*encryption.SIVNameCipher, 3)
		for _, domain := range []string{keyDomainID, keyDomainIndex, keyDomainKV} {
			cipher, err := newNameCipher(keys[0], domain)
			if err != nil {
				return nil, err
			}
			state.sivs[domain] = cipher
		}
	}

	return state, nil
}

// newNameCipher derives the deterministic key cipher for one domain.
func newNameCipher(masterKey []byte, domain string) (*encryption.SIVNameCipher, error) {
	return encryption.NewSIVNameCipher(masterKey, sivInfoPrefix+domain+"/v1")
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
