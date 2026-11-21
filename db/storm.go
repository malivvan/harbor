// Package db is a toolkit for BoltDB that provides indexes, a wide range
// of methods to store and fetch data, an advanced query system, and managed
// transactions with context support.
package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"

	"github.com/malivvan/storm/codec"
	"github.com/malivvan/storm/codec/json"
	bolt "go.etcd.io/bbolt"
)

const (
	dbinfo         = "__storm_db"
	metadataBucket = "__storm_metadata"
)

// BucketNamer is an interface that can be implemented by structs to provide
// a custom bucket name. This is essential for runtime-generated types
// (via reflect.StructOf) that have no static type name.
type BucketNamer interface {
	StormBucketName() string
}

// Defaults to json
var defaultCodec = json.Codec

// Open initializes Storm on an already-open bbolt database.
//
// The caller owns db and must keep it open for the lifetime of the returned
// Store. Storm never closes db. root is the optional root bucket key; nil or an
// empty slice means the database root. c is the codec used to encode and
// decode entities; nil selects the default JSON codec.
func Open(ctx context.Context, db *bolt.DB, root []byte, c codec.MarshalUnmarshaler) (*Store, error) {
	if err := checkContext(ctx); err != nil {
		return nil, wrapError("open", err)
	}
	if db == nil {
		return nil, wrapError("open", ErrNilParam)
	}

	if c == nil {
		c = defaultCodec
	}

	var rootBucket []string
	if len(root) > 0 {
		rootBucket = []string{string(root)}
	}

	s := Store{
		bolt: db,
	}

	n := node{
		s:          &s,
		codec:      c,
		rootBucket: rootBucket,
	}

	s.Node = &n

	if err := s.checkVersion(ctx); err != nil {
		return nil, wrapError("open", err)
	}

	return &s, nil
}

// Store is the wrapper around BoltDB. It contains an instance of BoltDB and uses it to perform all the
// needed operations
type Store struct {
	// The root node that points to the root bucket.
	Node

	// bolt is the underlying bbolt database. It is owned by the caller.
	bolt *bolt.DB
}

// NativeDB returns the underlying bbolt database.
//
// This is an advanced interoperability escape hatch. Native operations bypass
// Storm context checkpoints. Native writes can bypass codecs, indexes,
// metadata, and invariants. Storm cannot guarantee cancellation, rollback
// composition, index consistency, or destination safety for native operations.
// Callers must not close the returned database while Storm is in use.
// Callers are responsible for coordinating native transactions with Storm
// operations. Normal application code should prefer Storm APIs and managed
// transactions.
func (s *Store) NativeDB() *bolt.DB {
	if s == nil {
		return nil
	}
	return s.bolt
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
	raw, err := s.GetBytes(ctx, dbinfo, "version")
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return s.Set(ctx, dbinfo, "version", Version)
		}
		return err
	}

	var v string
	err = s.Codec().Unmarshal(raw, &v)
	if err != nil {
		return errors.Join(ErrDifferentCodec, err)
	}
	if v == "" {
		return s.Set(ctx, dbinfo, "version", Version)
	}

	return nil
}

// toBytes turns an interface into a slice of bytes
func toBytes(key interface{}, codec codec.MarshalUnmarshaler) ([]byte, error) {
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
		return codec.Marshal(key)
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
