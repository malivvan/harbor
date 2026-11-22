//go:generate go run main.go

package storm

import (
	"crypto/rand"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
	"golang.org/x/crypto/argon2"
)

// Encryption flags. Every bit enables encryption for one part of a sub-store;
// aggregate flags combine the bits of a whole sub-store.
const (
	EncryptNone uint16 = 0
)

const (
	EncryptDBKeys uint16 = 1 << iota
	EncryptDBValues
	EncryptDBMetadata
	EncryptFSContent
	EncryptFSMetadata
	EncryptTSValues
	EncryptTSSeriesNames
	EncryptVecPointIDs
	EncryptVecCollectionNames
	EncryptVecPoints
	EncryptDB  = EncryptDBKeys | EncryptDBValues | EncryptDBMetadata
	EncryptFS  = EncryptFSContent | EncryptFSMetadata
	EncryptTS  = EncryptTSValues | EncryptTSSeriesNames
	EncryptVec = EncryptVecPointIDs | EncryptVecCollectionNames | EncryptVecPoints
	EncryptAll = EncryptDB | EncryptFS | EncryptTS | EncryptVec
)

// hasFlag reports whether all bits in other are set in flags.
func hasFlag(flags, other uint16) bool {
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

// Named Argon2id parameter sets. They are individual constants rather than a
// struct so options can select them without allocating a configuration value.
const (
	// FastKDF* is for tests and short-lived development data. It is
	// deliberately cheap and is not suitable for protecting real secrets.
	FastKDFTime    uint32 = 1
	FastKDFMemory  uint32 = 64 * MiB
	FastKDFThreads uint8  = 1

	// DefaultKDF* follows RFC 9106's second recommended Argon2id setting
	// (t=3, m=64 MiB, p=4) and is what a new store uses.
	DefaultKDFTime    uint32 = 3
	DefaultKDFMemory  uint32 = 64 * MiB
	DefaultKDFThreads uint8  = 4

	// StrongKDF* is intended for high-value secrets on capable hardware
	// (t=4, m=256 MiB, p=8).
	StrongKDFTime    uint32 = 4
	StrongKDFMemory  uint32 = 256 * MiB
	StrongKDFThreads uint8  = 8
)

// validateKDFParams rejects parameters that would make Argon2id panic or that
// contain unknown encryption bits.
func validateKDFParams(time, memory uint32, threads uint8, flags uint16, base error) error {
	if time == 0 || memory == 0 || threads == 0 {
		return fmt.Errorf("%w: KDF time, memory and threads must be positive", base)
	}
	if memory < 8*uint32(threads) {
		return fmt.Errorf("%w: KDF memory must be at least %d KiB", base, 8*uint32(threads))
	}
	if flags&^EncryptAll != 0 {
		return fmt.Errorf("%w: unknown encryption flags %#x", base, flags)
	}
	return nil
}

// newSalt returns a fresh random KDF salt.
func newSalt() ([]byte, error) {
	salt := make([]byte, SaltSize)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("storm: generate KDF salt: %w", err)
	}
	return salt, nil
}

// deriveSeed stretches password with Argon2id and returns a 32-byte seed.
func deriveSeed(password, salt []byte, time, memory uint32, threads uint8) []byte {
	return argon2.IDKey(password, salt, time, memory, threads, KeySize)
}

// Metadata is stored as one unencrypted binco record in its own bucket. Keeping
// it under a single name makes the pre-decrypt configuration easy to find and
// leaves no other header or KDF bookkeeping outside it.
var (
	metadataBucket    = []byte("storm.metadata")
	metadataRecordKey = []byte("metadata")
)

const (
	// canaryPlaintext is sealed with a key derived from the password and
	// stored in the metadata record. Opening the store verifies it, so a wrong
	// password is rejected even when the sub-stores are not encrypted.
	canaryPlaintext = "storm/canary/v1"
)

// ErrCorruptStore reports missing or malformed store metadata.
var ErrCorruptStore = errors.New("storm: corrupt store metadata")

// Metadata is the unencrypted store configuration. It contains no secret: the
// salt and cost parameters are public, and the canary is ciphertext. Its binco
// serializer is generated into meta.gen.go; keep the mirror type in main.go in
// sync with this field order.
type Metadata struct {
	Salt    []byte
	Time    uint32
	Flags   uint16
	Memory  uint32
	Threads uint8
	Canary  []byte
}

// readMetadata loads and validates the unencrypted store configuration. It
// reports creating when the metadata bucket has not been written yet.
func readMetadata(bdb *bolt.DB) (md Metadata, creating bool, err error) {
	var raw []byte
	err = bdb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(metadataBucket)
		if b == nil {
			creating = true
			return nil
		}
		value := b.Get(metadataRecordKey)
		if value == nil {
			return fmt.Errorf("%w: missing metadata", ErrCorruptStore)
		}
		raw = append([]byte(nil), value...)
		return nil
	})
	if err != nil || creating {
		return md, creating, err
	}

	decoded, n, err := MetadataBinco.Unmarshal(raw)
	if err != nil {
		return md, false, fmt.Errorf("%w: decode metadata: %v", ErrCorruptStore, err)
	}
	if n != len(raw) {
		return md, false, fmt.Errorf("%w: trailing metadata bytes", ErrCorruptStore)
	}
	md = decoded
	if err := validateMetadata(md); err != nil {
		return md, false, err
	}
	return md, false, nil
}

func validateMetadata(md Metadata) error {
	if len(md.Salt) != SaltSize {
		return fmt.Errorf("%w: KDF salt must be %d bytes, got %d", ErrCorruptStore, SaltSize, len(md.Salt))
	}
	if len(md.Canary) == 0 {
		return fmt.Errorf("%w: empty canary", ErrCorruptStore)
	}
	return validateKDFParams(md.Time, md.Memory, md.Threads, md.Flags, ErrCorruptStore)
}

// writeMetadata persists the metadata and creates the key/value bucket in one
// transaction.
func writeMetadata(bdb *bolt.DB, md Metadata) error {
	raw := make([]byte, MetadataBinco.Size(md))
	if n := MetadataBinco.Marshal(md, raw); n != len(raw) {
		return fmt.Errorf("storm: encoded metadata is %d bytes, want %d", n, len(raw))
	}
	err := bdb.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists(metadataBucket)
		if err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(kvBucket); err != nil {
			return err
		}
		return meta.Put(metadataRecordKey, raw)
	})
	if err != nil {
		return fmt.Errorf("storm: write store metadata: %w", err)
	}
	return nil
}

// KDFParams returns the persisted salt and Argon2id cost parameters. A closed
// store returns ErrClosed.
func (s *Store) KDFParams() (salt []byte, time, memory uint32, threads uint8, flags uint16, err error) {
	err = s.withDB(func(bdb *bolt.DB) error {
		md, creating, err := readMetadata(bdb)
		if err != nil {
			return err
		}
		if creating {
			return fmt.Errorf("%w: missing metadata", ErrCorruptStore)
		}
		salt, time, memory, threads, flags = md.Salt, md.Time, md.Memory, md.Threads, md.Flags
		return nil
	})
	return
}
