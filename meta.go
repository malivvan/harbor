package storm

import (
	"crypto/rand"
	"encoding/binary"
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
	EncryptKVValues
	EncryptDB  = EncryptDBKeys | EncryptDBValues | EncryptDBMetadata
	EncryptFS  = EncryptFSContent | EncryptFSMetadata
	EncryptTS  = EncryptTSValues | EncryptTSSeriesNames
	EncryptVec = EncryptVecPointIDs | EncryptVecCollectionNames | EncryptVecPoints
	EncryptKV  = EncryptKVValues
	EncryptAll = EncryptDB | EncryptFS | EncryptTS | EncryptVec | EncryptKV
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

// Bucket and record names of the unencrypted store configuration. Keeping it
// under a single record makes the pre-decrypt configuration easy to find and
// leaves no other header or KDF bookkeeping outside it.
var (
	metadataBucket    = []byte("storm.Metadata")
	metadataRecordKey = []byte("Metadata")
)

const (
	// canaryPlaintext is sealed with a key derived from the password and
	// stored in the Metadata record. Opening the store verifies it, so a wrong
	// password is rejected even when the sub-stores are not encrypted.
	//
	// The sealed plaintext also binds the persisted encryption flags (see
	// canaryPlaintextFor), so flipping a flag bit invalidates the canary.
	canaryPlaintext = "storm/canary/v1"

	// metadataFixedSize is salt | time | flags | memory | threads | canary
	// length. The canary bytes follow.
	metadataFixedSize = SaltSize + 4 + 2 + 4 + 1 + 4
)

// ErrCorruptStore reports missing or malformed store Metadata.
var ErrCorruptStore = errors.New("storm: corrupt store Metadata")

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

// encode serializes md as:
//
//	salt[16] | time[4] | flags[2] | memory[4] | threads[1] | canaryLen[4] | canary
//
// All integers are big-endian.
func (md Metadata) encode() []byte {
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

// decodeMetadata parses a record produced by Metadata.encode.
func decodeMetadata(raw []byte) (Metadata, error) {
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
	canary := append([]byte(nil), raw[off:]...)

	md := Metadata{
		Salt:    salt,
		Time:    timeValue,
		Flags:   flags,
		Memory:  memory,
		Threads: threads,
		Canary:  canary,
	}
	if err := validateMetadata(md); err != nil {
		return Metadata{}, err
	}
	return md, nil
}

// readMetadata loads and validates the unencrypted store configuration. It
// reports creating when the Metadata bucket has not been written yet.
func readMetadata(bdb *bolt.DB) (meta Metadata, init bool, err error) {
	var raw []byte
	err = bdb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(metadataBucket)
		if b == nil {
			// A missing Metadata bucket only means a brand-new store when the
			// database holds no buckets at all. Otherwise the metadata was
			// lost or the file is not a Storm store; initializing fresh
			// metadata would silently mask that.
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
			init = true
			return nil
		}
		value := b.Get(metadataRecordKey)
		if value == nil {
			return fmt.Errorf("%w: missing Metadata", ErrCorruptStore)
		}
		raw = append([]byte(nil), value...)
		return nil
	})
	if err != nil || init {
		return meta, init, err
	}
	meta, err = decodeMetadata(raw)
	if err != nil {
		return meta, false, err
	}
	return meta, false, nil
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

// writeMetadata persists the Metadata and creates the key/value bucket in one
// transaction.
func writeMetadata(bdb *bolt.DB, md Metadata) error {
	if err := validateMetadata(md); err != nil {
		return err
	}
	raw := md.encode()
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
		return fmt.Errorf("storm: write store Metadata: %w", err)
	}
	return nil
}

// Metadata returns the persisted salt, Argon2id cost parameters and encryption
// flags. A closed store returns ErrClosed.
func (s *Store) Metadata() (salt []byte, time, memory uint32, threads uint8, flags uint16, err error) {
	err = s.withDB(func(bdb *bolt.DB) error {
		meta, init, err := readMetadata(bdb)
		if err != nil {
			return err
		}
		if init {
			return fmt.Errorf("%w: missing Metadata", ErrCorruptStore)
		}
		salt, time, memory, threads, flags = meta.Salt, meta.Time, meta.Memory, meta.Threads, meta.Flags
		return nil
	})
	return
}
