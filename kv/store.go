// Package kv provides a bucket-oriented key/value store on top of Storm's db
// package.
//
// A Store manages multiple named Bucket values. Bucket instances are created
// lazily and cached, so repeated calls to Store.Bucket return the same value.
package kv

import (
	"context"
	"sync"

	"github.com/malivvan/storm/codec"
	stormdb "github.com/malivvan/storm/db"
	bolt "go.etcd.io/bbolt"
)

// Store manages named key/value buckets backed by a Storm database.
type Store struct {
	db    *stormdb.DB
	root  []byte
	codec codec.MarshalUnmarshaler

	mu      sync.RWMutex
	buckets map[string]*Bucket
	order   []string
}

// Open wraps an already-open bbolt database in a key/value Store.
//
// The caller owns bdb and must keep it open for the lifetime of the returned
// Store. Storm never closes bdb. root is an optional root bucket key; nil or an
// empty slice means the database root. c is the codec used for values; nil
// selects the default JSON codec.
func Open(ctx context.Context, bdb *bolt.DB, root []byte, c codec.MarshalUnmarshaler) (*Store, error) {
	d, err := stormdb.Open(ctx, bdb, root, c)
	if err != nil {
		return nil, err
	}

	return &Store{
		db:      d,
		root:    cloneBytes(root),
		codec:   d.Codec(),
		buckets: make(map[string]*Bucket),
	}, nil
}

// DB returns the underlying Storm database wrapper.
func (s *Store) DB() *stormdb.DB {
	if s == nil {
		return nil
	}
	return s.db
}

// NativeDB returns the underlying bbolt database.
func (s *Store) NativeDB() *bolt.DB {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.NativeDB()
}

// Codec returns the codec used by the store.
func (s *Store) Codec() codec.MarshalUnmarshaler {
	if s == nil || s.db == nil {
		return nil
	}
	return s.codec
}

// Root returns a copy of the store's root bucket key.
func (s *Store) Root() []byte {
	if s == nil {
		return nil
	}
	return cloneBytes(s.root)
}

// Close releases the Store wrapper. The underlying bbolt database remains open
// and must be closed by its owner.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return stormdb.ErrNilParam
	}
	return s.db.Close()
}

// Bucket returns the named bucket. Bucket values are created lazily and cached
// by name; repeated calls return the same instance. An empty name addresses the
// store's root bucket.
func (s *Store) Bucket(name string) *Bucket {
	if s == nil || s.db == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if b, ok := s.buckets[name]; ok {
		return b
	}

	node := s.db.Node
	if name != "" {
		node = s.db.From(name)
	}

	b := &Bucket{
		store: s,
		name:  name,
		node:  node,
	}
	s.buckets[name] = b
	s.order = append(s.order, name)
	return b
}

// Buckets returns the bucket values created through this Store in creation
// order.
func (s *Store) Buckets() []*Bucket {
	if s == nil {
		return nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	buckets := make([]*Bucket, 0, len(s.order))
	for _, name := range s.order {
		buckets = append(buckets, s.buckets[name])
	}
	return buckets
}

// BucketNames returns the names of the bucket values created through this
// Store in creation order.
func (s *Store) BucketNames() []string {
	if s == nil {
		return nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, len(s.order))
	copy(names, s.order)
	return names
}

// DeleteBucket deletes a named bucket from the database and removes it from the
// Store's cache. An empty name is rejected with ErrNilParam.
func (s *Store) DeleteBucket(ctx context.Context, name string) error {
	if s == nil || s.db == nil {
		return stormdb.ErrNilParam
	}
	if name == "" {
		return stormdb.ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return err
	}

	err := s.db.NativeDB().Update(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		var parent *bolt.Bucket
		if len(s.root) > 0 {
			parent = tx.Bucket(s.root)
			if parent == nil {
				return stormdb.ErrNotFound
			}
		}

		if parent == nil {
			if tx.Bucket([]byte(name)) == nil {
				return stormdb.ErrNotFound
			}
			return tx.DeleteBucket([]byte(name))
		}

		if parent.Bucket([]byte(name)) == nil {
			return stormdb.ErrNotFound
		}
		return parent.DeleteBucket([]byte(name))
	})
	if err != nil {
		return err
	}

	s.mu.Lock()
	delete(s.buckets, name)
	for i, cached := range s.order {
		if cached == name {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	s.mu.Unlock()

	return nil
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return stormdb.ErrNilContext
	}
	return ctx.Err()
}
