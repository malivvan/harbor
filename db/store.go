// Package db is a toolkit for BoltDB that provides indexes, a wide range
// of methods to store and fetch data, an advanced query system, and managed
// transactions with context support.
package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	bolt "go.etcd.io/bbolt"
)

const (
	databaseInfo   = "__database"
	metadataBucket = "__metadata"
	versionKey     = "version"
)

// BucketNamer is an interface that can be implemented by structs to provide
// a custom bucket name. This is essential for runtime-generated types
// (via reflect.StructOf) that have no static type name.
type BucketNamer interface {
	StormBucketName() string
}

// defaultCodec is the codec used when none is supplied.
var defaultCodec = JSONCodec

// Open initializes Storm on an already-open bbolt database.
//
// The caller owns db and must keep it open for the lifetime of the returned
// Store. Storm never closes db.
//
// cfg configures the store. cfg.Root is an optional bucket path; an empty path
// means the database root. A zero cfg selects the default JSON codec and
// creates a plaintext store. cfg.Codec is the codec used to encode and decode
// entities; a nil Codec selects the default JSON codec.
//
// When cfg enables encryption the store writes an unencrypted marker into the
// __database bucket and verifies it with a sealed canary, so:
//
//   - opening an encrypted store without a Config (or with the wrong settings)
//     fails with ErrWrongMode,
//   - opening an encrypted store with the wrong key fails with ErrAuthFailed,
//   - opening a store that already holds plaintext data with encryption
//     requested fails with ErrWrongMode.
//
// Migrating an existing plaintext store to an encrypted one is not supported
// yet; export and re-import instead.
func Open(ctx context.Context, bdb *bolt.DB, cfg Config) (*Store, error) {
	if err := checkContext(ctx); err != nil {
		return nil, wrapError("open", err)
	}
	if bdb == nil {
		return nil, wrapError("open", ErrNilParam)
	}

	c := defaultCodec
	if cfg.Codec != nil {
		c = cfg.Codec
	}

	enc, err := resolveConfig(cfg)
	if err != nil {
		return nil, wrapError("open", err)
	}

	s := &Store{bolt: bdb}
	s.enc.Store(enc)

	root := cloneBucketPath(cfg.Root)
	n := node{
		s:     s,
		codec: c,
		root:  cloneBucketPath(root),
	}

	s.Node = &n

	if err := s.checkMode(ctx); err != nil {
		return nil, wrapError("open", err)
	}

	if err := s.checkVersion(ctx); err != nil {
		return nil, wrapError("open", err)
	}

	return s, nil
}

// Store is the wrapper around BoltDB. It contains an instance of BoltDB and uses it to perform all the
// needed operations
type Store struct {
	// The root node that points to the root bucket.
	Node

	// bolt is the underlying bbolt database. It is owned by the caller.
	bolt *bolt.DB

	// enc is the optional encryption state. It is an atomic pointer so a
	// rotation can swap the keys without racing readers.
	enc atomic.Pointer[encState]

	// rotMu serializes rotation against normal operations. Top-level
	// transactions take RLock; ReEncrypt/RotateAll take Lock across the
	// rewrite and the adoption of the new key state.
	rotMu sync.RWMutex
}

// Close releases the Storm wrapper. The underlying bbolt database is owned by
// the caller and is not closed.
//
// If the receiver is nil or the underlying bbolt database is nil, Close returns
// ErrNilParam without panicking.
func (s *Store) Close() error {
	if s == nil {
		return wrapError("close", ErrNilParam)
	}
	if s.bolt == nil {
		return wrapError("close", ErrNilParam)
	}
	return nil
}

func (s *Store) checkVersion(ctx context.Context) error {
	// Read the encoded value separately so a decode failure can be classified
	// as an incompatible codec while preserving the underlying codec error.
	root, ok := s.Node.(*node)
	if !ok {
		return ErrBadType
	}

	raw, err := root.getBytesRaw(ctx, databaseInfo, versionKey)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return s.Set(ctx, databaseInfo, versionKey, Version)
		}
		return err
	}

	var v string
	err = root.unmarshal(raw, &v)
	if err != nil {
		if errors.Is(err, ErrAuthFailed) {
			return err
		}
		return errors.Join(ErrDifferentCodec, err)
	}
	if v == "" {
		return s.Set(ctx, databaseInfo, versionKey, Version)
	}

	return nil
}

// checkMode reconciles the encryption settings of the Store with the marker
// stored on disk. A plaintext store has no marker; an encrypted store always
// has one, which also proves the configured key can decrypt the store.
func (s *Store) checkMode(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	root, ok := s.Node.(*node)
	if !ok {
		return ErrBadType
	}

	m, canary, found, err := root.loadMarker(ctx)
	if err != nil {
		return err
	}

	enc := s.enc.Load()
	if enc == nil {
		if found {
			return fmt.Errorf("%w: store is encrypted", ErrWrongMode)
		}
		return nil
	}

	if found {
		if !m.matches(enc) {
			return fmt.Errorf("%w: encryption settings do not match the store", ErrWrongMode)
		}
		return enc.verifyCanary(canary)
	}

	// No marker: only a brand-new store may be initialized as encrypted.
	hasData, err := root.hasUserData(ctx)
	if err != nil {
		return err
	}
	if hasData {
		return fmt.Errorf("%w: store already contains plaintext data", ErrWrongMode)
	}

	return root.storeMarker(ctx, enc)
}

// toBytes turns an interface into a slice of bytes
func toBytes(key interface{}, c Codec) ([]byte, error) {
	if key == nil {
		return nil, nil
	}
	switch t := key.(type) {
	case []byte:
		return t, nil
	case string:
		return []byte(t), nil
	case int:
		return numbertob(int64(t))
	case uint:
		return numbertob(uint64(t))
	case int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		return numbertob(t)
	default:
		return c.Marshal(key)
	}
}

func numbertob(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func numberfromb(raw []byte) (int64, error) {
	r := bytes.NewReader(raw)
	var to int64
	err := binary.Read(r, binary.BigEndian, &to)
	if err != nil {
		return 0, err
	}
	return to, nil
}
