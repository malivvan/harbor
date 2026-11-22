package ts

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/malivvan/harbor/internal/crypto"
	bolt "go.etcd.io/bbolt"
)

// KeySize is the required key length in bytes (256 bits).
const KeySize = crypto.KeySize

// Config enables optional encryption for a time series Store. A zero Config,
// or one with no encryption feature enabled, leaves the store in its plaintext
// format.
type Config struct {
	// Root is the bucket path below which all series live. Each element is one
	// nested bucket name. A nil or empty Root means the database root; the
	// harbor root package passes [][]byte{[]byte("ts")} explicitly.
	//
	// Encryption requires a non-empty Root: at the database root the store
	// cannot distinguish its own series buckets from unrelated top-level
	// buckets, so it cannot safely detect existing plaintext data or enumerate
	// series for rotation.
	Root [][]byte

	// Secret is a raw 32-byte master key. It must be set when encryption is
	// enabled.
	Secret []byte

	// EncryptValues seals point values and series metadata.
	EncryptValues bool

	// EncryptSeriesNames encrypts series bucket and metadata names with
	// deterministic AES-SIV.
	EncryptSeriesNames bool

	// BlockDuration groups points into encrypted time blocks of this width in
	// nanoseconds. It must be non-negative; zero disables block mode.
	//
	// With EncryptValues and high write/scan volume, block mode is strongly
	// recommended: each block is sealed and opened once, whereas non-block mode
	// seals every point separately. A duration around a few seconds to a
	// minute is a good default for streaming metrics.
	BlockDuration int64
}

func (c Config) encryptionEnabled() bool {
	return c.EncryptValues || c.EncryptSeriesNames || c.BlockDuration > 0
}

// encState is the resolved encryption state of a store: the cipher engines
// (through a Keychain) plus the enabled features. A nil keychain means
// plaintext, and every helper below is then a passthrough.
type encState struct {
	kc        *crypto.Keychain
	encValues bool
	encNames  bool
	blockDur  int64
	siv       *crypto.SIVNameCipher
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

func resolveConfig(cfg Config) (*encState, error) {
	if !cfg.encryptionEnabled() {
		return nil, nil
	}
	if cfg.BlockDuration < 0 {
		return nil, fmt.Errorf("%w: block duration cannot be negative", ErrInvalidConfig)
	}

	key, err := crypto.ResolveKey(cfg.Secret)
	if err != nil {
		return nil, err
	}

	kc, err := crypto.NewKeychain([][]byte{key})
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
		state.siv, err = crypto.NewSIVNameCipher(key, sivInfo)
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
	versionKey      = "\x00__ts_version__"
	markerMagic     = "TENC"
	canaryPlaintext = "harbor/ts/aead/v1"
)

const (
	markerFlagValues = 1 << 0
	markerFlagNames  = 1 << 1
)

// markerSpec describes the on-disk marker framing shared by every store.
var markerSpec = crypto.MarkerSpec{Magic: markerMagic, ExtraFieldSize: 8}

// marker describes the encryption mode recorded in a store.
type marker struct {
	encValues bool
	encNames  bool
	blockDur  int64
}

// flagsByte returns the marker bits for the configured state.
func (m marker) flagsByte() byte {
	var f byte
	if m.encValues {
		f |= markerFlagValues
	}
	if m.encNames {
		f |= markerFlagNames
	}
	return f
}

func (m marker) matches(s *encState) bool {
	return m.encValues == s.encValues &&
		m.encNames == s.encNames &&
		m.blockDur == s.blockDur
}

// encodeMarker serializes the marker with a sealed canary for key verification.
func (e *encState) encodeMarker() ([]byte, error) {
	var flags byte
	if e.encValues {
		flags |= markerFlagValues
	}
	if e.encNames {
		flags |= markerFlagNames
	}

	canary, err := e.seal([]byte(crypto.CanaryPlaintext(canaryPlaintext, flags, uint64(e.blockDur))))
	if err != nil {
		return nil, err
	}

	return crypto.EncodeMarker(markerSpec, crypto.Marker{
		Flags:  flags,
		Extra:  uint64(e.blockDur),
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
		encValues: decoded.Flags&markerFlagValues != 0,
		encNames:  decoded.Flags&markerFlagNames != 0,
		blockDur:  int64(decoded.Extra),
	}, decoded.Canary, nil
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
func (e *encState) verifyCanary(canary []byte, m marker) error {
	return crypto.VerifyCanary(e.kc, canary, crypto.CanaryPlaintext(canaryPlaintext, m.flagsByte(), uint64(m.blockDur)))
}

// ensureMarker writes the encryption marker if the store does not have one yet.
// It is called inside write transactions for encrypted stores.
func (s *Store) ensureMarker(tx *bolt.Tx) error {
	enc := s.encState()
	if enc == nil {
		return nil
	}
	bucket, err := s.metaBucket(tx, true)
	if err != nil {
		return err
	}
	if bucket.Get([]byte(markerKey)) != nil {
		return nil
	}
	raw, err := enc.encodeMarker()
	if err != nil {
		return err
	}
	return bucket.Put([]byte(markerKey), raw)
}

// ensureFormatVersion validates the persisted format revision in the metadata
// bucket, writing the current one for a store created before the marker
// existed.
func (s *Store) ensureFormatVersion() error {
	return s.b.Update(func(tx *bolt.Tx) error {
		if err := s.ensureMarker(tx); err != nil {
			return err
		}
		bucket, err := s.metaBucket(tx, true)
		if err != nil {
			return err
		}
		raw := bucket.Get([]byte(versionKey))
		if raw == nil {
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], tsFormatVersion)
			return bucket.Put([]byte(versionKey), buf[:])
		}
		if len(raw) != 8 {
			return fmt.Errorf("%w: malformed version marker", ErrIncompatibleVersion)
		}
		if v := binary.BigEndian.Uint64(raw); v != tsFormatVersion {
			return fmt.Errorf("%w: timeseries format %d, this build supports %d", ErrIncompatibleVersion, v, tsFormatVersion)
		}
		return nil
	})
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
				if string(key) != markerKey && string(key) != versionKey {
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
		if root := s.rootBucket(tx); root != nil {
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

// Open wraps an already-open bbolt database in a time series Store.
//
// The caller owns db and must keep it open for the lifetime of the Store. The
// Store never closes db. cfg enables optional encryption; cfg.Root is an
// optional bucket path (an empty path means the database root), and a zero
// Config, or one with no encryption enabled, creates a plaintext store.
func Open(ctx context.Context, bdb *bolt.DB, cfg Config) (*Store, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if bdb == nil {
		return nil, ErrNilParam
	}

	// Drop empty path elements: they would make rootBucket and createRootBucket
	// disagree and could hand createSeriesBucket a nil root.
	var root [][]byte
	for _, part := range cfg.Root {
		if len(part) == 0 {
			continue
		}
		root = append(root, append([]byte(nil), part...))
	}

	settings, err := resolveConfig(cfg)
	if err != nil {
		return nil, err
	}
	if settings != nil && len(root) == 0 {
		// At the database root unrelated top-level buckets cannot be told
		// apart from series, so encryption and rotation are refused.
		return nil, fmt.Errorf("%w: encryption requires a non-empty Root", ErrInvalidConfig)
	}

	s := &Store{
		b:      bdb,
		root:   root,
		series: make(map[string]*Series),
	}
	s.now.Store(&nowProvider{fn: defaultNow})

	s.enc.Store(settings)

	existing, canary, found, err := s.readMarker(bdb)
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
		if err := s.encState().verifyCanary(canary, existing); err != nil {
			return nil, err
		}
		if err := s.ensureFormatVersion(); err != nil {
			return nil, err
		}
		return s, nil
	}

	if settings != nil {
		hasData, err := s.hasData(bdb)
		if err != nil {
			return nil, err
		}
		if hasData {
			return nil, fmt.Errorf("%w: store already contains plaintext data", ErrWrongMode)
		}
	}

	if err := s.ensureFormatVersion(); err != nil {
		return nil, err
	}
	return s, nil
}
