package vec

import (
	"fmt"

	"github.com/malivvan/storm/internal/encryption"
	"go.etcd.io/bbolt"
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

// Key domains. Deterministic key encryption is domain-separated, so the same
// logical value yields a different physical name in each context.
const (
	keyDomainID   = "id"
	keyDomainName = "name"
)

// sivInfoPrefix domain-separates the deterministic name ciphers.
const sivInfoPrefix = "storm/vec/keys/"

// encState is the shared encryption configuration of a Store. A nil encState
// means plaintext: every helper is then an exact passthrough to the binco,
// JSON and raw-key paths used today.
type encState struct {
	// kc holds the cipher engines: the primary key first, then rotation
	// fallbacks. A nil keychain means plaintext.
	kc *encryption.Keychain

	encPoints bool
	encIDs    bool
	encNames  bool

	// sivs holds the per-domain deterministic name ciphers derived from the
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
// given domain. The encoding has no '_' in its alphabet, so encrypted names
// stay usable both as bbolt keys and inside composite keys.
func (e *encState) sealName(plain string, domain string) ([]byte, error) {
	if e == nil {
		return []byte(plain), nil
	}
	if (domain == keyDomainID && !e.encIDs) || (domain == keyDomainName && !e.encNames) {
		return []byte(plain), nil
	}
	cipher, ok := e.sivs[domain]
	if !ok {
		return nil, fmt.Errorf("%w: unknown key domain %q", ErrInvalidConfig, domain)
	}
	return cipher.SealName([]byte(plain))
}

// openName reverses sealName.
func (e *encState) openName(cipherName []byte, domain string) (string, error) {
	if e == nil {
		return string(cipherName), nil
	}
	if (domain == keyDomainID && !e.encIDs) || (domain == keyDomainName && !e.encNames) {
		return string(cipherName), nil
	}
	cipher, ok := e.sivs[domain]
	if !ok {
		return "", fmt.Errorf("%w: unknown key domain %q", ErrInvalidConfig, domain)
	}
	plain, err := cipher.OpenName(cipherName)
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
	return m.cipher == e.cipher() &&
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
	raw = append(raw, markerVersion, byte(e.cipher()), flags)
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

	keys, err := encryption.ResolveKeys(cfg.Key, cfg.KeyProvider)
	if err != nil {
		return nil, err
	}

	kc, err := encryption.NewKeychain(cipherSuite, keys)
	if err != nil {
		return nil, err
	}

	state := &encState{
		kc:        kc,
		encPoints: cfg.EncryptPoints,
		encIDs:    cfg.EncryptPointIDs,
		encNames:  cfg.EncryptCollectionNames,
	}

	if state.encIDs || state.encNames {
		// Deterministic name encryption is derived from a single master key: a
		// fallback key could decrypt values but not reproduce the physical
		// names, so lookups would silently miss.
		if err := encryption.RequireSingleKey(keys); err != nil {
			return nil, fmt.Errorf("%w; use ReEncryptCollection or RotateAll to rotate", err)
		}
		state.sivs = make(map[string]*encryption.SIVNameCipher, 2)
		for _, domain := range []string{keyDomainID, keyDomainName} {
			cipher, err := newNameCipher(keys[0], domain)
			if err != nil {
				return nil, err
			}
			state.sivs[domain] = cipher
		}
	}

	return state, nil
}

// newNameCipher derives the deterministic name cipher for one domain.
func newNameCipher(masterKey []byte, domain string) (*encryption.SIVNameCipher, error) {
	return encryption.NewSIVNameCipher(masterKey, sivInfoPrefix+domain+"/v1")
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
func (s *Store) keyBytes(id string) ([]byte, error) {
	return s.enc.sealName(id, keyDomainID)
}

// bucketKey returns the stored name for a collection: the SIV-encrypted name
// when collection names are encrypted, the raw name otherwise.
func (s *Store) bucketKey(name string) ([]byte, error) {
	return s.enc.sealName(name, keyDomainName)
}

// logicalName turns a stored collection name back into the name the caller
// knows.
func (s *Store) logicalName(physical []byte) (string, error) {
	return s.enc.openName(physical, keyDomainName)
}
