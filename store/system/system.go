// Package system owns tempest's store header: the unencrypted metadata record,
// the Argon2id settings, the encryption and trace flag vocabulary, and the
// derivation of every per-store key from the password.
//
// The root tempest package uses it to open a store; the sub-stores only receive
// the derived keys through their Config values. Keeping the header in one place
// means the KDF, canary, key labels and flag bits cannot drift between stores.
package system

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"

	"github.com/malivvan/tempest/internal/argon2"
	"github.com/malivvan/tempest/internal/bucket"
	"github.com/malivvan/tempest/internal/crypto"
	"github.com/malivvan/tempest/internal/slip21"
	"github.com/malivvan/tempest/store/ledger"
	bolt "go.etcd.io/bbolt"
)

// Encryption flags. Every bit enables encryption for one part of a sub-store;
// aggregate flags combine the bits of a whole sub-store.
const (
	// EncryptNone disables encryption for a store created with Plaintext.
	EncryptNone uint16 = 0
)

const (
	// EncryptRecordKeys encrypts db primary keys, index values and key/value keys.
	EncryptRecordKeys uint16 = 1 << iota
	// EncryptRecordValues encrypts db record values and typed key/value values.
	EncryptRecordValues
	// EncryptRecordMetadata encrypts db auto-increment counters.
	EncryptRecordMetadata
	// EncryptFSTreeContent encrypts fstree content and symlink targets.
	EncryptFSTreeContent
	// EncryptFSTreeMetadata encrypts fstree inode blobs.
	EncryptFSTreeMetadata
	// EncryptSeriesValues encrypts ts point values and series metadata.
	EncryptSeriesValues
	// EncryptSeriesNames encrypts ts series bucket and metadata names.
	EncryptSeriesNames
	// EncryptVectorPointIDs encrypts vec point IDs.
	EncryptVectorPointIDs
	// EncryptVectorCollectionNames encrypts vec collection names.
	EncryptVectorCollectionNames
	// EncryptVectorPoints encrypts vec point blobs and collection metadata.
	EncryptVectorPoints
	// EncryptGraphValues encrypts graph records. New flags are appended so the
	// existing bit assignments stay stable.
	EncryptGraphValues
	// EncryptObjectValues encrypts object-store records.
	EncryptObjectValues
	// EncryptRecord combines every db encryption flag.
	EncryptRecord = EncryptRecordKeys | EncryptRecordValues | EncryptRecordMetadata
	// EncryptFSTree combines every fs encryption flag.
	EncryptFSTree = EncryptFSTreeContent | EncryptFSTreeMetadata
	// EncryptSeries combines every ts encryption flag.
	EncryptSeries = EncryptSeriesValues | EncryptSeriesNames
	// EncryptVector combines every vec encryption flag.
	EncryptVector = EncryptVectorPointIDs | EncryptVectorCollectionNames | EncryptVectorPoints
	// EncryptGraph combines every graph encryption flag.
	EncryptGraph = EncryptGraphValues
	// EncryptObject combines every object-store encryption flag.
	EncryptObject = EncryptObjectValues
	// EncryptAll enables encryption for every sub-store and is the default for
	// a new store that does not set Config.Encrypt.
	EncryptAll = EncryptRecord | EncryptFSTree | EncryptSeries | EncryptVector | EncryptGraph | EncryptObject
)

// Trace flags. Every bit enables the trace log for one sub-store. Unlike
// encryption flags they are a per-open runtime choice and are not persisted.
const (
	// TraceNone disables the trace log.
	TraceNone uint16 = 0

	// TraceRecord records db writes.
	TraceRecord uint16 = 1 << iota
	// TraceFSTree records fs writes.
	TraceFSTree
	// TraceSeries records ts writes.
	TraceSeries
	// TraceVector records vec writes.
	TraceVector
	// TraceGraphs records graph writes.
	TraceGraphs
	// TraceObject records object-store writes.
	TraceObject
	// TraceSearch records full-text writes.
	TraceSearch

	// TraceAll records writes for every sub-store.
	TraceAll = TraceRecord | TraceFSTree | TraceSeries | TraceVector | TraceGraphs | TraceObject | TraceSearch
)

// HasFlag reports whether all bits in other are set in flags.
func HasFlag(flags, other uint16) bool {
	return flags&other == other
}

const (
	// SaltSize is the length in bytes of a generated KDF salt.
	SaltSize = 16
	// KeySize is the length in bytes of a key produced by the KDF.
	KeySize = 32
	// MiB is one mebibyte in KiB, the unit used by argon2 for Memory.
	MiB = 1024
)

// Named Argon2id parameter sets.
const (
	// FastKDF* is for tests and short-lived development data.
	FastKDFTime    uint32 = 1
	FastKDFMemory  uint32 = 64 * MiB
	FastKDFThreads uint8  = 1

	// DefaultKDF* follows RFC 9106's second recommended Argon2id setting.
	DefaultKDFTime    uint32 = 3
	DefaultKDFMemory  uint32 = 64 * MiB
	DefaultKDFThreads uint8  = 4

	// StrongKDF* is intended for high-value secrets on capable hardware.
	StrongKDFTime    uint32 = 4
	StrongKDFMemory  uint32 = 256 * MiB
	StrongKDFThreads uint8  = 8
)

const (
	// Maximum accepted Argon2id parameters. The salt and cost parameters are
	// stored unauthenticated, so out-of-range values must be rejected before
	// deriving.
	MaxKDFTime    uint32 = 100
	MaxKDFMemory  uint32 = 1 * 1024 * 1024 // KiB, 1 GiB
	MaxKDFThreads uint8  = 64
)

// Errors returned by the package.
var (
	// ErrCorruptStore reports missing or malformed store metadata.
	ErrCorruptStore = errors.New("tempest: corrupt store Metadata")
	// ErrInvalidPassword reports an empty password or a failed canary.
	ErrInvalidPassword = errors.New("tempest: invalid password")
	// ErrInvalidOptions reports an unusable header configuration.
	ErrInvalidOptions = errors.New("tempest: invalid options")
)

// Metadata bucket and record names. The header lives in its own top-level
// bucket, named by the internal/bucket registry.
var (
	metadataBucket    = []byte(bucket.SYSTEM)
	metadataRecordKey = []byte("Metadata")
)

const (
	canaryPlaintext = "tempest/canary/v1"

	// metadataFixedSize is salt | time | flags | memory | threads | canary length.
	metadataFixedSize = SaltSize + 4 + 2 + 4 + 1 + 4
)

// Metadata is the unencrypted store configuration. It contains no secret: the
// salt and cost parameters are public, and the canary is ciphertext.
type Metadata struct {
	Salt    []byte
	Time    uint32
	Flags   uint16
	Memory  uint32
	Threads uint8
	Canary  []byte
}

// Encode serializes md.
func (md Metadata) Encode() []byte {
	buf := make([]byte, metadataFixedSize+len(md.Canary))
	copy(buf, md.Salt)
	off := SaltSize
	binary.BigEndian.PutUint32(buf[off:], md.Time)
	off += 4
	binary.BigEndian.PutUint16(buf[off:], md.Flags)
	off += 2
	binary.BigEndian.PutUint32(buf[off:], md.Memory)
	off += 4
	buf[off] = md.Threads
	off++
	binary.BigEndian.PutUint32(buf[off:], uint32(len(md.Canary)))
	off += 4
	copy(buf[off:], md.Canary)
	return buf
}

// DecodeMetadata parses a record produced by Metadata.Encode.
func DecodeMetadata(raw []byte) (Metadata, error) {
	if len(raw) < metadataFixedSize {
		return Metadata{}, fmt.Errorf("%w: truncated Metadata", ErrCorruptStore)
	}
	off := 0
	salt := append([]byte(nil), raw[:SaltSize]...)
	off += SaltSize
	timeValue := binary.BigEndian.Uint32(raw[off:])
	off += 4
	flags := binary.BigEndian.Uint16(raw[off:])
	off += 2
	memory := binary.BigEndian.Uint32(raw[off:])
	off += 4
	threads := raw[off]
	off++
	canaryLen := binary.BigEndian.Uint32(raw[off:])
	off += 4
	if uint64(canaryLen) != uint64(len(raw)-off) {
		return Metadata{}, fmt.Errorf("%w: Metadata canary length mismatch", ErrCorruptStore)
	}
	md := Metadata{
		Salt:    salt,
		Time:    timeValue,
		Flags:   flags,
		Memory:  memory,
		Threads: threads,
		Canary:  append([]byte(nil), raw[off:]...),
	}
	if err := ValidateMetadata(md); err != nil {
		return Metadata{}, err
	}
	return md, nil
}

// ReadMetadata loads and validates the unencrypted store configuration. It
// reports creating when the Metadata bucket has not been written yet.
func ReadMetadata(bdb *bolt.DB) (meta Metadata, creating bool, err error) {
	var raw []byte
	err = bdb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(metadataBucket)
		if b == nil {
			empty := true
			if err := tx.ForEach(func([]byte, *bolt.Bucket) error {
				empty = false
				return nil
			}); err != nil {
				return err
			}
			if !empty {
				return fmt.Errorf("%w: missing Metadata bucket in a non-empty database", ErrCorruptStore)
			}
			creating = true
			return nil
		}
		value := b.Get(metadataRecordKey)
		if value == nil {
			return fmt.Errorf("%w: missing Metadata", ErrCorruptStore)
		}
		raw = append([]byte(nil), value...)
		return nil
	})
	if err != nil || creating {
		return meta, creating, err
	}
	meta, err = DecodeMetadata(raw)
	return meta, false, err
}

// ValidateMetadata checks the salt, canary and KDF parameters.
func ValidateMetadata(md Metadata) error {
	if len(md.Salt) != SaltSize {
		return fmt.Errorf("%w: KDF salt must be %d bytes, got %d", ErrCorruptStore, SaltSize, len(md.Salt))
	}
	if len(md.Canary) == 0 {
		return fmt.Errorf("%w: empty canary", ErrCorruptStore)
	}
	return ValidateKDFParams(md.Time, md.Memory, md.Threads, md.Flags, ErrCorruptStore)
}

// WriteMetadata persists the Metadata in one transaction.
func WriteMetadata(bdb *bolt.DB, md Metadata) error {
	if err := ValidateMetadata(md); err != nil {
		return err
	}
	raw := md.Encode()
	if err := bdb.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(metadataBucket)
		if err != nil {
			return err
		}
		return b.Put(metadataRecordKey, raw)
	}); err != nil {
		return fmt.Errorf("tempest: write store Metadata: %w", err)
	}
	return nil
}

// ValidateKDFParams rejects parameters that would make Argon2id panic or that
// contain unknown encryption bits.
func ValidateKDFParams(time, memory uint32, threads uint8, flags uint16, base error) error {
	if time == 0 || memory == 0 || threads == 0 {
		return fmt.Errorf("%w: KDF time, memory and threads must be positive", base)
	}
	if memory < 8*uint32(threads) {
		return fmt.Errorf("%w: KDF memory must be at least %d KiB", base, 8*uint32(threads))
	}
	if time > MaxKDFTime || memory > MaxKDFMemory || threads > MaxKDFThreads {
		return fmt.Errorf("%w: KDF parameters exceed the supported maximum", base)
	}
	if flags&^EncryptAll != 0 {
		return fmt.Errorf("%w: unknown encryption flags %#x", base, flags)
	}
	return nil
}

// NewSalt returns a fresh random KDF salt.
func NewSalt() ([]byte, error) {
	salt := make([]byte, SaltSize)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("tempest: generate KDF salt: %w", err)
	}
	return salt, nil
}

// DeriveSeed stretches password with Argon2id and returns a 32-byte seed.
func DeriveSeed(password, salt []byte, time, memory uint32, threads uint8) []byte {
	return argon2.IDKey(password, salt, time, memory, threads, KeySize)
}

// Config is the header configuration for a new or existing store.
type Config struct {
	Password   string
	KDFTime    uint32
	KDFMemory  uint32
	KDFThreads uint8
	Encrypt    uint16
	Plaintext  bool
	Trace      uint16
}

// Validate checks the options that apply when a store is created.
func (c Config) Validate() error {
	if c.Password == "" {
		return fmt.Errorf("%w: password is required", ErrInvalidPassword)
	}
	if _, _, _, _, err := c.creationParams(); err != nil {
		return err
	}
	if unknown := c.Trace &^ TraceAll; unknown != 0 {
		return fmt.Errorf("%w: unknown trace flags %#x", ErrInvalidOptions, unknown)
	}
	return nil
}

// creationParams resolves the KDF parameters and encryption flags for a new
// store.
func (c Config) creationParams() (time, memory uint32, threads uint8, flags uint16, err error) {
	time, memory, threads = c.KDFTime, c.KDFMemory, c.KDFThreads
	switch {
	case time == 0 && memory == 0 && threads == 0:
		time, memory, threads = DefaultKDFTime, DefaultKDFMemory, DefaultKDFThreads
	case time == 0 || memory == 0 || threads == 0:
		return 0, 0, 0, 0, fmt.Errorf("%w: incomplete KDF parameters", ErrInvalidOptions)
	}
	flags, err = c.encryptionFlags()
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if err := ValidateKDFParams(time, memory, threads, flags, ErrInvalidOptions); err != nil {
		return 0, 0, 0, 0, err
	}
	return time, memory, threads, flags, nil
}

func (c Config) encryptionFlags() (uint16, error) {
	if c.Plaintext {
		if c.Encrypt != 0 {
			return 0, fmt.Errorf("%w: Plaintext conflicts with encryption flags", ErrInvalidOptions)
		}
		return EncryptNone, nil
	}
	flags := c.Encrypt
	if flags == 0 {
		flags = EncryptAll
	}
	return flags, nil
}

// CanaryPlaintextFor returns the flag-bound canary plaintext.
func CanaryPlaintextFor(flags uint16) string {
	return fmt.Sprintf("%s/0x%04x", canaryPlaintext, flags)
}

// SealCanary seals the flag-bound canary with the canary key.
func SealCanary(key []byte, flags uint16) ([]byte, error) {
	keychain, err := canaryKeychain(key)
	if err != nil {
		return nil, fmt.Errorf("tempest: initialize canary key: %w", err)
	}
	sealed, err := keychain.Seal([]byte(CanaryPlaintextFor(flags)))
	if err != nil {
		return nil, fmt.Errorf("tempest: seal canary: %w", err)
	}
	return sealed, nil
}

// VerifyCanary proves that key decrypts the stored canary and that the
// persisted flags are the ones it was sealed with.
func VerifyCanary(key, sealed []byte, flags uint16) error {
	keychain, err := canaryKeychain(key)
	if err != nil {
		return err
	}
	return cryptoVerifyCanary(keychain, sealed, CanaryPlaintextFor(flags))
}

func canaryKeychain(key []byte) (*crypto.Keychain, error) {
	return crypto.NewKeychain([][]byte{key})
}

func cryptoVerifyCanary(keychain *crypto.Keychain, sealed []byte, want string) error {
	return crypto.VerifyCanary(keychain, sealed, want)
}

// Key labels. Each value is a fixed /v1 derivation domain that is persisted
// indirectly through the derived keys: changing one changes every key it
// derives. The project rename from Harbor to tempest renamed these from
// "harbor/..." to "tempest/...", which is why stores written before the rename
// cannot be opened by this build.
const (
	LabelCanary = "tempest/canary/v1"
	LabelRecord = "tempest/db/v1"
	LabelFSTree = "tempest/fs/v1"
	LabelSeries = "tempest/ts/v1"
	LabelVector = "tempest/vec/v1"
	LabelSearch = "tempest/fts/v1"
	LabelGraphs = "tempest/graphs/v1"
	LabelObject = "tempest/object/v1"
)

// SLIP-10 hardened paths for the per-store trace signing keys.
const (
	TracePathRecord = "m/0'"
	TracePathFSTree = "m/1'"
	TracePathSeries = "m/2'"
	TracePathVector = "m/3'"
	TracePathGraphs = "m/4'"
	TracePathObject = "m/5'"
	TracePathSearch = "m/6'"
)

// Keys holds the per-component keys derived for one open call. Wipe it when the
// sub-stores have copied the key material.
type Keys struct {
	Canary []byte
	Record []byte
	FSTree []byte
	Series []byte
	Vector []byte
	Search []byte
	Graphs []byte
	Object []byte

	TraceRecord ed25519.PrivateKey
	TraceFSTree ed25519.PrivateKey
	TraceSeries ed25519.PrivateKey
	TraceVector ed25519.PrivateKey
	TraceGraphs ed25519.PrivateKey
	TraceObject ed25519.PrivateKey
	TraceSearch ed25519.PrivateKey
}

// Wipe overwrites the byte key material.
func (k *Keys) Wipe() {
	if k == nil {
		return
	}
	for _, b := range [][]byte{k.Canary, k.Record, k.FSTree, k.Series, k.Vector, k.Search, k.Graphs, k.Object,
		k.TraceRecord, k.TraceFSTree, k.TraceSeries, k.TraceVector, k.TraceGraphs, k.TraceObject, k.TraceSearch} {
		wipe(b)
	}
}

func wipe(b []byte) {
	for i := range b {
		b[i] = 0
	}
	runtime.KeepAlive(b)
}

// Derive stretches the password and derives every component key.
func Derive(password string, salt []byte, time, memory uint32, threads uint8) (*Keys, error) {
	passwordBytes := []byte(password)
	defer wipe(passwordBytes)

	seed := DeriveSeed(passwordBytes, salt, time, memory, threads)
	defer wipe(seed)

	master, err := slip21.New(seed)
	if err != nil {
		return nil, fmt.Errorf("tempest: derive master key: %w", err)
	}
	defer master.Destroy()

	derive := func(label string) ([]byte, error) {
		node, err := master.Derive([]byte(label))
		if err != nil {
			return nil, fmt.Errorf("tempest: derive %s key: %w", label, err)
		}
		defer node.Destroy()
		key := node.Key()
		if len(key) != KeySize {
			return nil, fmt.Errorf("tempest: derived %s key has %d bytes", label, len(key))
		}
		return key, nil
	}

	keys := &Keys{}
	labels := []struct {
		label string
		dst   *[]byte
	}{
		{LabelCanary, &keys.Canary},
		{LabelRecord, &keys.Record},
		{LabelFSTree, &keys.FSTree},
		{LabelSeries, &keys.Series},
		{LabelVector, &keys.Vector},
		{LabelSearch, &keys.Search},
		{LabelGraphs, &keys.Graphs},
		{LabelObject, &keys.Object},
	}
	for _, item := range labels {
		*item.dst, err = derive(item.label)
		if err != nil {
			keys.Wipe()
			return nil, err
		}
	}

	tracePaths := []struct {
		path string
		dst  *ed25519.PrivateKey
	}{
		{TracePathRecord, &keys.TraceRecord},
		{TracePathFSTree, &keys.TraceFSTree},
		{TracePathSeries, &keys.TraceSeries},
		{TracePathVector, &keys.TraceVector},
		{TracePathGraphs, &keys.TraceGraphs},
		{TracePathObject, &keys.TraceObject},
		{TracePathSearch, &keys.TraceSearch},
	}
	for _, item := range tracePaths {
		_, priv, err := ledger.DeriveKey(seed, item.path)
		if err != nil {
			keys.Wipe()
			return nil, err
		}
		*item.dst = priv
	}
	return keys, nil
}

// LoadOrCreate reads the Metadata and derives the component keys. A new store
// resolves cfg into Metadata, seals the canary and persists it; an existing
// store derives keys from the stored parameters and verifies the canary. The
// returned keys must be wiped by the caller.
func LoadOrCreate(bdb *bolt.DB, cfg Config) (Metadata, *Keys, error) {
	if bdb == nil {
		return Metadata{}, nil, fmt.Errorf("%w: database is required", ErrInvalidOptions)
	}
	if unknown := cfg.Trace &^ TraceAll; unknown != 0 {
		return Metadata{}, nil, fmt.Errorf("%w: unknown trace flags %#x", ErrInvalidOptions, unknown)
	}
	if cfg.Password == "" {
		return Metadata{}, nil, fmt.Errorf("%w: password is required", ErrInvalidPassword)
	}
	md, creating, err := ReadMetadata(bdb)
	if err != nil {
		return Metadata{}, nil, err
	}
	if !creating {
		keys, err := Derive(cfg.Password, md.Salt, md.Time, md.Memory, md.Threads)
		if err != nil {
			return Metadata{}, nil, err
		}
		if err := VerifyCanary(keys.Canary, md.Canary, md.Flags); err != nil {
			keys.Wipe()
			return Metadata{}, nil, fmt.Errorf("%w: %w", ErrInvalidPassword, err)
		}
		return md, keys, nil
	}

	time, memory, threads, flags, err := cfg.creationParams()
	if err != nil {
		return Metadata{}, nil, err
	}
	salt, err := NewSalt()
	if err != nil {
		return Metadata{}, nil, err
	}
	keys, err := Derive(cfg.Password, salt, time, memory, threads)
	if err != nil {
		return Metadata{}, nil, err
	}
	canary, err := SealCanary(keys.Canary, flags)
	if err != nil {
		keys.Wipe()
		return Metadata{}, nil, err
	}
	md = Metadata{Salt: salt, Time: time, Flags: flags, Memory: memory, Threads: threads, Canary: canary}
	if err := WriteMetadata(bdb, md); err != nil {
		keys.Wipe()
		return Metadata{}, nil, err
	}
	return md, keys, nil
}
