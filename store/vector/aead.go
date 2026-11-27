package vector

import (
	"fmt"

	"github.com/malivvan/tempest/internal/compress"
	"github.com/malivvan/tempest/internal/crypto"
	"github.com/malivvan/tempest/metrics"
	"github.com/malivvan/tempest/store/ledger"
	"go.etcd.io/bbolt"
)

// KeySize is the required key length in bytes (256 bits).
const KeySize = crypto.KeySize

// Config configures a Store: it enables optional vector quantization and
// optional crypto. A zero Config, or one with no quantization and no
// encryption feature enabled, keeps the plaintext on-disk format.
type Config struct {
	// Root is the bucket path below which all collections live. Each element
	// is one nested bucket name. A nil or empty Root means the database root;
	// the tempest root package passes [][]byte{[]byte(store.VECTOR)} explicitly.
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

	// Compress Zstd-compresses point blobs and collection metadata on the
	// plaintext side of encryption. It is a write-time preference: reads
	// auto-detect the envelope, so a store written compressed stays readable
	// with Compress disabled. It does not require encryption.
	Compress bool

	// CompressLevel selects the Zstandard level (0 selects the default).
	CompressLevel int

	// MinCompress skips compressing values smaller than this many bytes.
	MinCompress int

	// Audit, when non-nil, receives an entry for every committed write.
	Audit ledger.Sink

	// SharedDB reports that the caller owns the bbolt handle. Store.Close then
	// flushes buffered points and releases the wrapper without closing the
	// handle, which is what an embedding store (the tempest root package) needs.
	// It is equivalent to calling SetOwnsDB(false) after Open. The zero value
	// keeps standalone ownership: Close closes the handle.
	SharedDB bool

	// Metrics, when non-nil, receives search/flush counters, candidate counts
	// and tracing spans. A nil Observer disables instrumentation.
	Metrics metrics.Observer
}

func (c Config) encryptionEnabled() bool {
	return c.EncryptPoints || c.EncryptPointIDs || c.EncryptCollectionNames
}

// Validate reports whether the configuration can be opened. Encryption needs a
// valid secret; UseQuant without a Quantizer is valid because Normalize fills in
// the default SQ8 quantizer.
func (c Config) Validate() error {
	if !c.encryptionEnabled() {
		return nil
	}
	_, err := crypto.ResolveKey(c.Secret)
	return err
}

// Normalize returns the configuration with defaults applied and validates it:
// UseQuant selects the default quantizer when none is set, and empty Root
// elements are dropped. Open calls Normalize, so the public validation and the
// open path cannot drift.
func (c Config) Normalize() (Config, error) {
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	if c.UseQuant && c.Quantizer == nil {
		c.Quantizer = NewSQ8Quantizer()
	}
	if len(c.Root) > 0 {
		root := make([][]byte, 0, len(c.Root))
		for _, part := range c.Root {
			if len(part) == 0 {
				continue
			}
			root = append(root, append([]byte(nil), part...))
		}
		c.Root = root
	}
	return c, nil
}

// Key domains. Deterministic key encryption is domain-separated, so the same
// logical value yields a different physical name in each context.
const (
	keyDomainID   = "id"
	keyDomainName = "name"
)

// sivInfoPrefix domain-separates the deterministic name ciphers.
const sivInfoPrefix = "tempest/vec/keys/"

// encState is the shared encryption configuration of a Store. A nil encState
// means plaintext: every helper is then an exact passthrough to the point
// JSON and raw-key paths used today.
type encState struct {
	// kc holds the cipher engines: the primary key first, then rotation
	// fallbacks. A nil keychain means plaintext.
	kc *crypto.Keychain

	encPoints bool
	encIDs    bool
	encNames  bool

	// sivs holds the per-domain deterministic name ciphers derived from the
	// primary key. Read-only after construction.
	sivs map[string]*crypto.SIVNameCipher

	// comp compresses point blobs and metadata before sealing; nil when
	// compression is off. Immutable and shared across rotation states.
	comp *compress.Codec
}

// encrypted reports whether a real cipher is configured. A state can exist for
// compression alone, with no keychain.
func (e *encState) encrypted() bool { return e != nil && e.kc != nil }

// seal encrypts plain with the primary engine, prefixing a fresh random nonce.
func (e *encState) seal(plain []byte) ([]byte, error) {
	if !e.encrypted() {
		return plain, nil
	}
	return e.kc.Seal(plain)
}

// open decrypts raw with the primary engine and any rotation fallbacks.
func (e *encState) open(raw []byte) ([]byte, error) {
	if !e.encrypted() {
		return raw, nil
	}
	return e.kc.Open(raw)
}

// sealPoints compresses a point blob or collection metadata when compression
// is enabled, then seals it when EncryptPoints is enabled.
func (e *encState) sealPoints(plain []byte) ([]byte, error) {
	if e != nil && e.comp != nil {
		var err error
		if plain, err = e.comp.Encode(nil, plain); err != nil {
			return nil, err
		}
	}
	if e == nil || !e.encPoints || !e.encrypted() {
		return plain, nil
	}
	return e.seal(plain)
}

// openPoints reverses sealPoints and always decodes a compressed envelope.
func (e *encState) openPoints(raw []byte) ([]byte, error) {
	if e == nil || !e.encPoints || !e.encrypted() {
		return compress.Decode(nil, raw)
	}
	plain, err := e.open(raw)
	if err != nil {
		return nil, err
	}
	return compress.Decode(nil, plain)
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
	canaryPlaintext = "tempest/vec/aead/v1"
)

const (
	markerFlagPoints = 1 << 0
	markerFlagIDs    = 1 << 1
	markerFlagNames  = 1 << 2
)

// markerSpec describes the on-disk marker framing shared by every store.
var markerSpec = crypto.MarkerSpec{Magic: markerMagic}

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

	canary, err := e.seal([]byte(crypto.CanaryPlaintext(canaryPlaintext, flags, 0)))
	if err != nil {
		return nil, err
	}

	return crypto.EncodeMarker(markerSpec, crypto.Marker{
		Flags:  flags,
		Canary: canary,
	})
}

// decodeMarker parses a marker and returns its sealed canary.
func decodeMarker(raw []byte) (marker, []byte, error) {
	decoded, err := crypto.DecodeMarker(markerSpec, raw)
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
	return crypto.VerifyCanary(e.kc, canary, crypto.CanaryPlaintext(canaryPlaintext, m.flagsByte(), 0))
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
	if !cfg.encryptionEnabled() && !cfg.Compress {
		return nil, nil
	}

	state := &encState{}
	if cfg.Compress {
		codec, err := compress.NewCodec(compress.Config{Level: cfg.CompressLevel, MinSize: cfg.MinCompress})
		if err != nil {
			return nil, err
		}
		state.comp = codec
	}
	if !cfg.encryptionEnabled() {
		return state, nil
	}

	key, err := crypto.ResolveKey(cfg.Secret)
	if err != nil {
		return nil, err
	}

	kc, err := crypto.NewKeychain([][]byte{key})
	if err != nil {
		return nil, err
	}

	state.kc = kc
	state.encPoints = cfg.EncryptPoints || cfg.EncryptPointIDs
	state.encIDs = cfg.EncryptPointIDs
	state.encNames = cfg.EncryptCollectionNames

	if state.encIDs || state.encNames {
		state.sivs = make(map[string]*crypto.SIVNameCipher, 2)
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
func newNameCipher(masterKey []byte, domain string) (*crypto.SIVNameCipher, error) {
	return crypto.NewSIVNameCipher(masterKey, sivInfoPrefix+domain+"/v1")
}

// sealBytes seals a point blob or metadata payload. With no encryption
// configured it returns p untouched.
func (s *Store) sealBytes(p []byte) ([]byte, error) {
	return s.encState().sealPoints(p)
}

// openBytes reverses sealBytes.
func (s *Store) openBytes(c []byte) ([]byte, error) {
	return s.encState().openPoints(c)
}

// keyBytes returns the stored key for a point ID: the SIV-encrypted ID when
// point IDs are encrypted, the raw ID otherwise.
func (s *Store) keyBytes(id string) ([]byte, error) {
	return s.encState().sealName(id, keyDomainID)
}

// bucketKey returns the stored name for a collection: the SIV-encrypted name
// when collection names are encrypted, the raw name otherwise.
func (s *Store) bucketKey(name string) ([]byte, error) {
	return s.encState().sealName(name, keyDomainName)
}

// logicalName turns a stored collection name back into the name the caller
// knows.
func (s *Store) logicalName(physical []byte) (string, error) {
	return s.encState().openName(physical, keyDomainName)
}
