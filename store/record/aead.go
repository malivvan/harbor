package record

import (
	"context"
	"fmt"
	"reflect"

	"github.com/malivvan/eidos-db/internal/crypto"
	"github.com/malivvan/eidos-db/pkg/metrics"
	"github.com/malivvan/eidos-db/store/ledger"
	bolt "go.etcd.io/bbolt"
)

// KeySize is the required key length in bytes (256 bits).
const KeySize = crypto.KeySize

// Config configures a Store: it selects the codec and enables optional
// crypto. A zero Config, or one with no encryption feature enabled, keeps
// the plaintext on-disk format and uses the default JSON codec.
type Config struct {
	// Root is the bucket path below which all eidos-db data lives. Each element
	// is one nested bucket name. A nil or empty Root means the database root,
	// so eidos-db's buckets are top-level. The eidos root package passes
	// [][]byte{[]byte(store.RECORD)} explicitly.
	Root [][]byte

	// Codec encodes and decodes entities. A nil Codec selects the default JSON
	// codec.
	Codec Codec

	// Secret is a raw 32-byte master key. It must be set when encryption is
	// enabled.
	Secret []byte

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

	// Audit, when non-nil, receives an entry for every committed write.
	Audit ledger.Sink

	// Metrics, when non-nil, receives operation counters, transaction
	// latencies and tracing spans. A nil Observer disables instrumentation.
	Metrics metrics.Observer
}

func (c Config) encryptionEnabled() bool {
	return c.EncryptValues || c.EncryptMetadata || c.EncryptKeys
}

// Validate reports whether the configuration can be opened. It checks the same
// invariants Open enforces, without opening a database: an encryption secret is
// required (and must have a valid key length) when any encryption flag is set.
// A nil Codec is valid and selects the default codec.
func (c Config) Validate() error {
	if !c.encryptionEnabled() {
		return nil
	}
	_, err := crypto.ResolveKey(c.Secret)
	return err
}

// Normalize returns the configuration with defaults applied and validates it. A
// nil Codec selects the default JSON codec. Open calls Normalize, so the public
// validation and the open path cannot drift.
func (c Config) Normalize() (Config, error) {
	if c.Codec == nil {
		c.Codec = defaultCodec
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// Key domains. Every deterministic key encryption is domain-separated, so the
// same logical value yields a different physical key in each context.
const (
	keyDomainID    = "id"
	keyDomainIndex = "index"
	keyDomainKV    = "kv"
)

// sivInfoPrefix domain-separates the deterministic key ciphers.
const sivInfoPrefix = "eidos/db/keys/"

// encState is the shared encryption configuration of a Store. Reachable from
// every node through its Store pointer. A nil encState means plaintext: every
// helper is then an exact passthrough to the codec and toBytes.
type encState struct {
	// kc holds the cipher engines: the primary key first, then rotation
	// fallbacks. A nil keychain means plaintext.
	kc *crypto.Keychain

	encValues   bool
	encMetadata bool
	encKeys     bool

	// sivs holds the per-domain deterministic name ciphers, derived from the
	// primary key. Read-only after construction.
	sivs map[string]*crypto.SIVNameCipher
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
	if e == nil || !e.encValues {
		return plain, nil
	}
	return e.seal(plain)
}

// openValues reverses sealValues.
func (e *encState) openValues(raw []byte) ([]byte, error) {
	if e == nil || !e.encValues {
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
	canaryPlaintext = "eidos/db/aead/v1"
)

const (
	markerFlagValues   = 1 << 0
	markerFlagMetadata = 1 << 1
	markerFlagKeys     = 1 << 2
)

// markerSpec describes the on-disk marker framing shared by every store.
var markerSpec = crypto.MarkerSpec{Magic: markerMagic}

// marker describes the encryption mode recorded in a store.
type marker struct {
	encValues   bool
	encMetadata bool
	encKeys     bool
}

// flagsByte returns the marker bits for the configured state.
func (m marker) flagsByte() byte {
	var f byte
	if m.encValues {
		f |= markerFlagValues
	}
	if m.encMetadata {
		f |= markerFlagMetadata
	}
	if m.encKeys {
		f |= markerFlagKeys
	}
	return f
}

func (m marker) matches(e *encState) bool {
	return m.encValues == e.encValues &&
		m.encMetadata == e.encMetadata &&
		m.encKeys == e.encKeys
}

// encodeMarker serializes the marker with a sealed canary used to verify the
// key: opening the canary with the wrong key fails with ErrAuthFailed.
func encodeMarker(e *encState) ([]byte, error) {
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
		encValues:   decoded.Flags&markerFlagValues != 0,
		encMetadata: decoded.Flags&markerFlagMetadata != 0,
		encKeys:     decoded.Flags&markerFlagKeys != 0,
	}, decoded.Canary, nil
}

// verifyCanary confirms the configured keys decrypt the stored canary.
func (e *encState) verifyCanary(canary []byte, m marker) error {
	return crypto.VerifyCanary(e.kc, canary, crypto.CanaryPlaintext(canaryPlaintext, m.flagsByte(), 0))
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
		// The reserved entries of __database are written by eidos-db itself:
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
// zero Config and for a Config that enables no encryption feature.
func resolveConfig(cfg Config) (*encState, error) {
	if !cfg.encryptionEnabled() {
		return nil, nil
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
		kc:          kc,
		encValues:   cfg.EncryptValues,
		encMetadata: cfg.EncryptMetadata,
		encKeys:     cfg.EncryptKeys,
	}

	if state.encKeys {
		state.sivs = make(map[string]*crypto.SIVNameCipher, 3)
		for _, domain := range []string{keyDomainID, keyDomainIndex, keyDomainKV} {
			cipher, err := newNameCipher(key, domain)
			if err != nil {
				return nil, err
			}
			state.sivs[domain] = cipher
		}
	}

	return state, nil
}

// newNameCipher derives the deterministic key cipher for one domain.
func newNameCipher(masterKey []byte, domain string) (*crypto.SIVNameCipher, error) {
	return crypto.NewSIVNameCipher(masterKey, sivInfoPrefix+domain+"/v1")
}

// encFromNode returns the encryption state of the Store that owns n, or nil
// for a plaintext store.
func encFromNode(n Node) *encState {
	nd, ok := n.(*node)
	if !ok || nd.s == nil {
		return nil
	}
	return nd.s.encState()
}

// nodeEnc returns the encryption state of the node, or nil for a plaintext
// store.
func (n *node) nodeEnc() *encState {
	if n == nil || n.s == nil {
		return nil
	}
	return n.s.encState()
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
//
// The destination is zeroed before decoding. Codecs such as encoding/json and
// gob only overwrite fields present in the payload, so a reused destination
// (ReIndex, batched scans, OrderBy) would otherwise inherit values from the
// previously decoded record.
func (n *node) unmarshal(raw []byte, v any) error {
	plain, err := n.open(raw)
	if err != nil {
		return err
	}
	if dest := reflect.ValueOf(v); dest.IsValid() && dest.Kind() == reflect.Pointer && !dest.IsNil() {
		dest.Elem().Set(reflect.Zero(dest.Elem().Type()))
	}
	if len(n.project) > 0 {
		return unmarshalProjected(n.codec, plain, n.project, v)
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

// coerceKeyValue converts a lookup value to the declared type of the indexed
// field when both sides are in the same kind class (numeric or identical).
// Without this an int lookup on a uint field would produce a different key
// than the stored uint field.
func coerceKeyValue(v any, field *fieldConfig) any {
	if v == nil || field == nil || field.Value == nil {
		return v
	}
	rv := reflect.ValueOf(v)
	ft := field.Value.Type()
	if !rv.IsValid() || !rv.Type().ConvertibleTo(ft) {
		return v
	}
	if !sameKindClass(rv.Kind(), ft.Kind()) {
		return v
	}
	return rv.Convert(ft).Interface()
}

// sameKindClass reports whether two kinds may be treated as the same value
// class for index-key coercion.
func sameKindClass(a, b reflect.Kind) bool {
	numeric := func(k reflect.Kind) bool {
		return (k >= reflect.Int && k <= reflect.Int64) ||
			(k >= reflect.Uint && k <= reflect.Uint64) ||
			k == reflect.Float32 || k == reflect.Float64
	}
	if numeric(a) || numeric(b) {
		return numeric(a) && numeric(b)
	}
	return a == b
}
