package storm

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/malivvan/storm/codec/json"
	"github.com/malivvan/storm/fs"
	"github.com/malivvan/storm/ts"
	"github.com/malivvan/storm/vec"

	"os"
	"sync"

	"github.com/malivvan/storm/db"
	bolt "go.etcd.io/bbolt"
)

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

func Open(ctx context.Context, path string, mode os.FileMode, options *bolt.Options) (s *Store, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	s = &Store{ctx: ctx}
	s.b, err = bolt.Open(path, mode, options)
	if err != nil {
		return nil, err
	}
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

	s.db, err = db.Open(ctx, s.b, []byte{2}, json.Codec)
	if err != nil {
		return nil, err
	}

	s.vec, err = vec.Open(ctx, s.b, []byte{3}, false, nil)
	if err != nil {
		return nil, err
	}

	s.fs, err = fs.Open(ctx, s.b, []byte{4})
	if err != nil {
		return nil, err
	}

	s.ts, err = ts.Open(ctx, s.b, []byte{5})
	if err != nil {
		return nil, err
	}

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

func (s *Store) Get(key string) string {
	if len(key) == 0 {
		return ""
	}
	var val string
	err := s.b.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(_null)
		if b == nil {
			return errors.New("null bucket not found")
		}
		v := b.Get([]byte(key))
		if v == nil {
			return errors.New("key not found")
		}
		val = string(v)
		return nil
	})
	if err != nil {
		panic(err)
	}
	return val
}

func (s *Store) Set(key, val string) error {
	if len(key) == 0 {
		return errors.New("key is empty")
	}
	return s.b.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(_null)
		if b == nil {
			return errors.New("null bucket not found")
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
