package vec

import (
	"fmt"

	"github.com/malivvan/storm/internal/encryption"
	"go.etcd.io/bbolt"
)

// KeySize is the required key length in bytes (256 bits).
const KeySize = encryption.KeySize

// Config configures a Store: it enables optional vector quantization and
// optional encryption. A zero Config, or one with no quantization and no
// encryption feature enabled, keeps the plaintext on-disk format.
type Config struct {
	// Root is the bucket path below which all collections live. Each element
	// is one nested bucket name. A nil or empty Root means the database root;
	// the storm root package passes [][]byte{[]byte("vec")} explicitly.
	Root [][]byte

	// UseQuant enables vector quantization. When true and Quantizer is nil,
	// NewSQ8Quantizer is used.
	UseQuant bool

	// Quantizer compresses vectors when UseQuant is enabled.
	Quantizer Quantizer

	// Secret is a raw 32-byte master key. It must be set when encryption is
	// enabled.
	Secret []byte

	// EncryptPoints seals point blobs (vectors, payloads and quantized vectors)
	// and collection metadata.
	EncryptPoints bool

	// EncryptPointIDs deterministically encrypts point IDs with AES-SIV, so
	// exact lookups and deletes keep working while the stored keys no longer
	// reveal them. It also encrypts the stored point value: the ID is part of
	// that value, and encrypting only the key would leave it in plaintext.
	EncryptPointIDs bool

	// EncryptCollectionNames deterministically encrypts collection bucket and
	// metadata names with AES-SIV. Listing still returns the original names.
	EncryptCollectionNames bool
}

func (c Config) encryptionEnabled() bool {
	return c.EncryptPoints || c.EncryptPointIDs || c.EncryptCollectionNames
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
// means plaintext: every helper is then an exact passthrough to the point
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
	if e == nil || !e.encPoints {
		return plain, nil
	}
	return e.seal(plain)
}

// openPoints reverses sealPoints.
func (e *encState) openPoints(raw []byte) ([]byte, error) {
	if e == nil || !e.encPoints {
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
	canaryPlaintext = "storm/vec/aead/v1"
)

const (
	markerFlagPoints = 1 << 0
	markerFlagIDs    = 1 << 1
	markerFlagNames  = 1 << 2
)

// markerSpec describes the on-disk marker framing shared by every store.
var markerSpec = encryption.MarkerSpec{Magic: markerMagic}

// marker describes the encryption mode recorded in a store.
type marker struct {
	encPoints bool
	encIDs    bool
	encNames  bool
}

// flagsByte returns the marker bits for the configured state.
func (m marker) flagsByte() byte {
	var f byte
	if m.encPoints {
		f |= markerFlagPoints
	}
	if m.encIDs {
		f |= markerFlagIDs
	}
	if m.encNames {
		f |= markerFlagNames
	}
	return f
}

func (m marker) matches(e *encState) bool {
	return m.encPoints == e.encPoints &&
		m.encIDs == e.encIDs &&
		m.encNames == e.encNames
}

// encodeMarker serializes the marker with a sealed canary used to verify the
// key: opening the canary with the wrong key fails with ErrAuthFailed.
func encodeMarker(e *encState) ([]byte, error) {
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

	canary, err := e.seal([]byte(encryption.CanaryPlaintext(canaryPlaintext, flags, 0)))
	if err != nil {
		return nil, err
	}

	return encryption.EncodeMarker(markerSpec, encryption.Marker{
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
		encPoints: decoded.Flags&markerFlagPoints != 0,
		encIDs:    decoded.Flags&markerFlagIDs != 0,
		encNames:  decoded.Flags&markerFlagNames != 0,
	}, decoded.Canary, nil
}

// verifyCanary confirms the configured keys decrypt the stored canary.
func (e *encState) verifyCanary(canary []byte, m marker) error {
	return encryption.VerifyCanary(e.kc, canary, encryption.CanaryPlaintext(canaryPlaintext, m.flagsByte(), 0))
}

// loadMarker reads the store marker, if present.
func (s *Store) loadMarker() (marker, []byte, bool, error) {
	var (
		m      marker
		canary []byte
		found  bool
	)
	err := s.view(func(tx *bbolt.Tx) error {
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
	return s.update(func(tx *bbolt.Tx) error {
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
	err := s.view(func(tx *bbolt.Tx) error {
		if len(s.root) > 0 {
			root := s.rootBucket(tx)
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
	err := s.view(func(tx *bbolt.Tx) error {
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
// zero Config and for a Config that enables no encryption feature.
func resolveConfig(cfg Config) (*encState, error) {
	if !cfg.encryptionEnabled() {
		return nil, nil
	}

	key, err := encryption.ResolveKey(cfg.Secret)
	if err != nil {
		return nil, err
	}

	kc, err := encryption.NewKeychain([][]byte{key})
	if err != nil {
		return nil, err
	}

	state := &encState{
		kc: kc,
		// The ID is stored inside the point value, so encrypting only the key
		// would leave it readable.
		encPoints: cfg.EncryptPoints || cfg.EncryptPointIDs,
		encIDs:    cfg.EncryptPointIDs,
		encNames:  cfg.EncryptCollectionNames,
	}

	if state.encIDs || state.encNames {
		state.sivs = make(map[string]*encryption.SIVNameCipher, 2)
		for _, domain := range []string{keyDomainID, keyDomainName} {
			cipher, err := newNameCipher(key, domain)
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
	return s.enc.Load().sealPoints(p)
}

// openBytes reverses sealBytes.
func (s *Store) openBytes(c []byte) ([]byte, error) {
	return s.enc.Load().openPoints(c)
}

// keyBytes returns the stored key for a point ID: the SIV-encrypted ID when
// point IDs are encrypted, the raw ID otherwise.
func (s *Store) keyBytes(id string) ([]byte, error) {
	return s.enc.Load().sealName(id, keyDomainID)
}

// bucketKey returns the stored name for a collection: the SIV-encrypted name
// when collection names are encrypted, the raw name otherwise.
func (s *Store) bucketKey(name string) ([]byte, error) {
	return s.enc.Load().sealName(name, keyDomainName)
}

// logicalName turns a stored collection name back into the name the caller
// knows.
func (s *Store) logicalName(physical []byte) (string, error) {
	return s.enc.Load().openName(physical, keyDomainName)
}
