package ts

import (
	"context"
	"encoding/binary"
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

// Config enables optional encryption for a time series Store. A nil Config, or
// one with no encryption feature enabled, leaves the store in its plaintext
// format.
type Config struct {
	// Cipher selects the AEAD cipher. CipherAuto (the default) resolves to
	// AES-256-GCM.
	Cipher CipherSuite

	// Key is a raw 32-byte master key. Exactly one of Key and KeyProvider must
	// be set when encryption is enabled.
	Key []byte

	// KeyProvider supplies the master key instead of Key.
	KeyProvider KeyProvider

	// EncryptValues seals point values and series metadata.
	EncryptValues bool

	// EncryptSeriesNames encrypts series bucket and metadata names with
	// deterministic AES-SIV.
	EncryptSeriesNames bool

	// BlockDuration groups points into encrypted time blocks of this width in
	// nanoseconds. It must be non-negative; zero disables block mode.
	BlockDuration int64
}

func (c *Config) encryptionEnabled() bool {
	return c != nil && (c.EncryptValues || c.EncryptSeriesNames || c.BlockDuration > 0)
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

// encState is the resolved encryption state of a store: the cipher engines
// (through a Keychain) plus the enabled features. A nil keychain means
// plaintext, and every helper below is then a passthrough.
type encState struct {
	kc        *encryption.Keychain
	encValues bool
	encNames  bool
	blockDur  int64
	siv       *encryption.SIVNameCipher
}

// cipher returns the resolved cipher suite.
func (e *encState) cipher() CipherSuite {
	if e == nil {
		return CipherAuto
	}
	return e.kc.Cipher()
}

// seal encrypts plain with the primary key, prepending a fresh nonce.
func (e *encState) seal(plain []byte) ([]byte, error) {
	if e == nil {
		return plain, nil
	}
	return e.kc.Seal(plain)
}

// open decrypts raw, trying the primary key and then any rotation fallbacks.
func (e *encState) open(raw []byte) ([]byte, error) {
	if e == nil {
		return raw, nil
	}
	return e.kc.Open(raw)
}

func resolveConfig(cfg *Config) (*encState, error) {
	if !cfg.encryptionEnabled() {
		return nil, nil
	}
	if cfg.BlockDuration < 0 {
		return nil, fmt.Errorf("%w: block duration cannot be negative", ErrInvalidConfig)
	}

	cipherSuite, err := encryption.Resolve(cfg.Cipher)
	if err != nil {
		return nil, ErrUnsupportedCipher
	}

	keys, err := encryption.ResolveKeys(cfg.Key, cfg.KeyProvider)
	if err != nil {
		return nil, err
	}

	if cfg.EncryptSeriesNames {
		// Deterministic series names derive from one master key: a fallback key
		// could decrypt values but never reproduce the physical names, so
		// lookups would silently miss.
		if err := encryption.RequireSingleKey(keys); err != nil {
			return nil, fmt.Errorf("%w; use ReEncryptSeries or RotateAll to rotate series names", err)
		}
	}

	kc, err := encryption.NewKeychain(cipherSuite, keys)
	if err != nil {
		return nil, err
	}

	state := &encState{
		kc:        kc,
		encValues: cfg.EncryptValues,
		encNames:  cfg.EncryptSeriesNames,
		blockDur:  cfg.BlockDuration,
	}
	if state.encNames {
		state.siv, err = encryption.NewSIVNameCipher(keys[0], sivInfo)
		if err != nil {
			return nil, err
		}
	}
	return state, nil
}

// Marker constants. The marker is stored unencrypted in the metadata bucket so
// a plaintext open can detect an encrypted store without a key.
const (
	markerKey       = "\x00__ts_enc__"
	markerMagic     = "TENC"
	markerVersion   = byte(1)
	canaryPlaintext = "storm/ts/aead/v1"
)

const (
	markerFlagValues = 1 << 0
	markerFlagNames  = 1 << 1
)

// marker describes the encryption mode recorded in a store.
type marker struct {
	cipher    CipherSuite
	encValues bool
	encNames  bool
	blockDur  int64
}

func (m marker) matches(s *encState) bool {
	return m.cipher == s.cipher() &&
		m.encValues == s.encValues &&
		m.encNames == s.encNames &&
		m.blockDur == s.blockDur
}

// encodeMarker serializes the marker with a sealed canary for key verification.
func (e *encState) encodeMarker() ([]byte, error) {
	canary, err := e.seal([]byte(canaryPlaintext))
	if err != nil {
		return nil, err
	}

	var flags byte
	if e.encValues {
		flags |= markerFlagValues
	}
	if e.encNames {
		flags |= markerFlagNames
	}

	raw := make([]byte, 0, 4+1+1+1+8+len(canary))
	raw = append(raw, markerMagic...)
	raw = append(raw, markerVersion, byte(e.cipher()), flags)
	var blockDur [8]byte
	binary.LittleEndian.PutUint64(blockDur[:], uint64(e.blockDur))
	raw = append(raw, blockDur[:]...)
	raw = append(raw, canary...)
	return raw, nil
}

// decodeMarker parses a marker and returns the sealed canary.
func decodeMarker(raw []byte) (marker, []byte, error) {
	const fixedSize = 4 + 1 + 1 + 1 + 8
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
		encValues: raw[6]&markerFlagValues != 0,
		encNames:  raw[6]&markerFlagNames != 0,
		blockDur:  int64(binary.LittleEndian.Uint64(raw[7:15])),
	}
	return m, raw[fixedSize:], nil
}

// readMarker returns the marker and its sealed canary, if the store has one.
func (s *Store) readMarker(db *bolt.DB) (marker, []byte, bool, error) {
	var (
		m      marker
		canary []byte
		found  bool
	)
	err := db.View(func(tx *bolt.Tx) error {
		bucket, err := s.metaBucket(tx, false)
		if err != nil {
			return err
		}
		if bucket == nil {
			return nil
		}
		raw := bucket.Get([]byte(markerKey))
		if raw == nil {
			return nil
		}
		m, canary, err = decodeMarker(raw)
		if err != nil {
			return err
		}
		found = true
		return nil
	})
	return m, canary, found, err
}

// verifyCanary checks that at least one configured key can decrypt the canary.
func (e *encState) verifyCanary(canary []byte) error {
	plaintext, err := e.open(canary)
	if err != nil {
		return err
	}
	if string(plaintext) != canaryPlaintext {
		return ErrAuthFailed
	}
	return nil
}

// ensureMarker writes the encryption marker if the store does not have one yet.
// It is called inside write transactions for encrypted stores.
func (s *Store) ensureMarker(tx *bolt.Tx) error {
	if s == nil || s.enc == nil {
		return nil
	}
	bucket, err := s.metaBucket(tx, true)
	if err != nil {
		return err
	}
	if bucket.Get([]byte(markerKey)) != nil {
		return nil
	}
	raw, err := s.enc.encodeMarker()
	if err != nil {
		return err
	}
	return bucket.Put([]byte(markerKey), raw)
}

// hasData reports whether the store already contains plaintext series data.
// A store with a non-empty root can inspect its series buckets; at the database
// root only the metadata bucket is distinguishable from unrelated buckets.
func (s *Store) hasData(db *bolt.DB) (bool, error) {
	var found bool
	err := db.View(func(tx *bolt.Tx) error {
		meta, err := s.metaBucket(tx, false)
		if err != nil {
			return err
		}
		if meta != nil {
			if err := meta.ForEach(func(key, _ []byte) error {
				if string(key) != markerKey {
					found = true
				}
				return nil
			}); err != nil {
				return err
			}
		}
		if found || len(s.root) == 0 {
			return nil
		}
		if root := tx.Bucket(s.root); root != nil {
			if err := root.ForEach(func(key, value []byte) error {
				if value == nil && string(key) != metaBucketName {
					found = true
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return found, err
}

// OpenWithConfig wraps an already-open bbolt database with the given optional
// encryption configuration. A nil config, or one with no encryption enabled, is
// equivalent to Open.
func OpenWithConfig(ctx context.Context, db *bolt.DB, root []byte, cfg *Config) (*Store, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if db == nil {
		return nil, ErrNilParam
	}

	settings, err := resolveConfig(cfg)
	if err != nil {
		return nil, err
	}

	s := &Store{
		b:      db,
		root:   append([]byte(nil), root...),
		now:    defaultNow,
		series: make(map[string]*Series),
	}

	s.enc = settings

	existing, canary, found, err := s.readMarker(db)
	if err != nil {
		return nil, err
	}
	if found {
		if settings == nil {
			return nil, fmt.Errorf("%w: store is encrypted", ErrWrongMode)
		}
		if !existing.matches(settings) {
			return nil, fmt.Errorf("%w: encryption settings do not match the store", ErrWrongMode)
		}
		if err := s.enc.verifyCanary(canary); err != nil {
			return nil, err
		}
		return s, nil
	}

	if settings != nil {
		hasData, err := s.hasData(db)
		if err != nil {
			return nil, err
		}
		if hasData {
			return nil, fmt.Errorf("%w: store already contains plaintext data", ErrWrongMode)
		}
	}

	return s, nil
}
