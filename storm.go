package storm

import (
	"context"
	"errors"
	"fmt"

	"os"
	"sync"

	"github.com/google/uuid"
	"github.com/malivvan/storm/fs"
	"github.com/malivvan/storm/ts"
	"github.com/malivvan/storm/vec"

	"github.com/malivvan/storm/db"
	bolt "go.etcd.io/bbolt"
)

// Config configures a Store.
type Config struct {
	// Secret is the raw 32-byte master key shared by the db, fs, ts and vec
	// sub-stores. It is required: every sub-store is opened encrypted.
	Secret []byte
}

type Store struct {
	b   *bolt.DB
	mu  sync.RWMutex
	id  uuid.UUID
	db  *db.Store
	fs  *fs.Store
	ts  *ts.Store
	vec *vec.Store
	ctx context.Context
}

var _null []byte      // sys
var _zero = []byte{0} // kv
var _one = []byte{1}  // db
var _two = []byte{2}

func Open(ctx context.Context, path string, mode os.FileMode, options *bolt.Options, cfg Config) (s *Store, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(cfg.Secret) != db.KeySize {
		return nil, fmt.Errorf("storm: master key must be %d bytes, got %d", db.KeySize, len(cfg.Secret))
	}
	s = &Store{ctx: ctx}
	s.b, err = bolt.Open(path, mode, options)
	if err != nil {
		return nil, err
	}
	bdb := s.b
	opened := false
	defer func() {
		if !opened {
			_ = bdb.Close()
		}
	}()
	err = s.b.Update(func(tx *bolt.Tx) (err error) {
		b := tx.Bucket([]byte{0})
		if b == nil {
			b, err = tx.CreateBucket([]byte{0})
			if err != nil {
				return err
			}
			s.id = uuid.New()
			idb, err := s.id.MarshalBinary()
			if err != nil {
				return err
			}
			err = b.Put([]byte{0}, idb)
			if err != nil {
				return err
			}
			return nil
		}
		idb := b.Get([]byte{0})
		if idb == nil {
			return nil
		}
		err = s.id.UnmarshalBinary(idb)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	secret := append([]byte(nil), cfg.Secret...)

	if s.db, err = db.Open(ctx, s.b, []byte{1}, db.Config{
		Secret:          secret,
		Codec:           db.JSONCodec,
		EncryptKeys:     true,
		EncryptValues:   true,
		EncryptMetadata: true,
	}); err != nil {
		return nil, err
	}
	if s.fs, err = fs.Open(ctx, s.b, []byte{2}, fs.Config{
		Secret:          secret,
		EncryptMetadata: true,
		EncryptContent:  true,
	}); err != nil {
		return nil, err
	}
	if s.ts, err = ts.Open(ctx, s.b, []byte{3}, ts.Config{
		Secret:             secret,
		EncryptValues:      true,
		EncryptSeriesNames: true,
	}); err != nil {
		return nil, err
	}
	if s.vec, err = vec.Open(ctx, s.b, []byte{4}, vec.Config{
		Secret:                 secret,
		UseQuant:               false,
		Quantizer:              nil,
		EncryptPoints:          true,
		EncryptPointIDs:        true,
		EncryptCollectionNames: true,
	}); err != nil {
		return nil, err
	}

	opened = true
	return s, nil
}

func (s *Store) ID() uuid.UUID {
	return s.id
}

func (s *Store) DB() *db.Store {
	return s.db
}

func (s *Store) Vec() *vec.Store {
	return s.vec
}

func (s *Store) FS() *fs.Store {
	return s.fs
}

func (s *Store) TS() *ts.Store {
	return s.ts
}

// ErrKeyNotFound is returned by Get when the key does not exist.
var ErrKeyNotFound = errors.New("storm: key not found")

// Get returns the value stored under key.
func (s *Store) Get(key string) (string, error) {
	if len(key) == 0 {
		return "", ErrKeyNotFound
	}
	var val string
	err := s.b.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(_zero)
		if b == nil {
			return errors.New("kv bucket not found")
		}
		v := b.Get([]byte(key))
		if v == nil {
			return ErrKeyNotFound
		}
		val = string(v)
		return nil
	})
	if err != nil {
		return "", err
	}
	return val, nil
}

func (s *Store) Set(key, val string) error {
	if len(key) == 0 {
		return errors.New("key is empty")
	}
	return s.b.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(_zero)
		if b == nil {
			return errors.New("kv bucket not found")
		}
		if val == "" {
			return b.Delete([]byte(key))
		}
		return b.Put([]byte(key), []byte(val))
	})
}

func (s *Store) Close() error {
	if s == nil || s.b == nil {
		return nil
	}
	return s.b.Close()
}
