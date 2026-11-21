package ts

import (
	"context"
	"sync"

	bolt "go.etcd.io/bbolt"
)

const metaBucketName = "__ts_meta__"

// Store manages named time series backed by a bbolt database.
type Store struct {
	b      *bolt.DB
	root   []byte
	mu     sync.RWMutex
	series map[string]*Series
	order  []string
}

// Open wraps an already-open bbolt database in a time series Store.
//
// The caller owns db and must keep it open for the lifetime of the Store. The
// Store never closes db. root is an optional root bucket key; nil or an empty
// slice means the database root.
func Open(ctx context.Context, db *bolt.DB, root []byte) (*Store, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if db == nil {
		return nil, ErrNilParam
	}

	return &Store{
		b:      db,
		root:   append([]byte(nil), root...),
		series: make(map[string]*Series),
	}, nil
}

// NativeDB returns the underlying bbolt database.
func (s *Store) NativeDB() *bolt.DB {
	if s == nil {
		return nil
	}
	return s.b
}

// Root returns a copy of the store's root bucket key.
func (s *Store) Root() []byte {
	if s == nil {
		return nil
	}
	return append([]byte(nil), s.root...)
}

// Close releases the Store wrapper. The underlying bbolt database remains open
// and must be closed by its owner.
func (s *Store) Close() error {
	if s == nil || s.b == nil {
		return ErrNilParam
	}
	return nil
}

// Series returns the named series, creating and caching it on demand.
func (s *Store) Series(name string) *Series {
	if s == nil || s.b == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if series, ok := s.series[name]; ok {
		return series
	}

	series := &Series{store: s, name: name}
	s.series[name] = series
	s.order = append(s.order, name)
	return series
}

// SeriesNames returns the names of series created through this Store, in
// creation order.
func (s *Store) SeriesNames() []string {
	if s == nil {
		return nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, len(s.order))
	copy(names, s.order)
	return names
}

// DeleteSeries drops a series bucket and its metadata.
func (s *Store) DeleteSeries(ctx context.Context, name string) error {
	if s == nil || s.b == nil {
		return ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if name == "" {
		return ErrNilParam
	}

	err := s.b.Update(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		found := false
		var parent *bolt.Bucket
		if len(s.root) > 0 {
			parent = tx.Bucket(s.root)
		}

		if parent == nil {
			if tx.Bucket([]byte(name)) != nil {
				if err := tx.DeleteBucket([]byte(name)); err != nil {
					return err
				}
				found = true
			}
		} else if parent.Bucket([]byte(name)) != nil {
			if err := parent.DeleteBucket([]byte(name)); err != nil {
				return err
			}
			found = true
		}

		meta, err := s.metaBucket(tx, false)
		if err != nil {
			return err
		}
		if meta != nil && meta.Get([]byte(name)) != nil {
			if err := meta.Delete([]byte(name)); err != nil {
				return err
			}
			found = true
		}

		if !found {
			return ErrNotFound
		}
		return checkContext(ctx)
	})
	if err != nil {
		return err
	}

	s.mu.Lock()
	delete(s.series, name)
	for i, cached := range s.order {
		if cached == name {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	s.mu.Unlock()

	return nil
}

func (s *Store) rootBucket(tx *bolt.Tx) *bolt.Bucket {
	if len(s.root) == 0 {
		return nil
	}
	return tx.Bucket(s.root)
}

func (s *Store) seriesBucket(tx *bolt.Tx, name string) *bolt.Bucket {
	if len(s.root) == 0 {
		return tx.Bucket([]byte(name))
	}

	root := s.rootBucket(tx)
	if root == nil {
		return nil
	}
	return root.Bucket([]byte(name))
}

func (s *Store) createSeriesBucket(tx *bolt.Tx, name string) (*bolt.Bucket, error) {
	if len(s.root) == 0 {
		return tx.CreateBucketIfNotExists([]byte(name))
	}

	root, err := tx.CreateBucketIfNotExists(s.root)
	if err != nil {
		return nil, err
	}
	return root.CreateBucketIfNotExists([]byte(name))
}

func (s *Store) metaBucket(tx *bolt.Tx, create bool) (*bolt.Bucket, error) {
	var parent *bolt.Bucket
	var err error

	if len(s.root) > 0 {
		if create {
			parent, err = tx.CreateBucketIfNotExists(s.root)
		} else {
			parent = tx.Bucket(s.root)
		}
		if err != nil {
			return nil, err
		}
		if parent == nil {
			return nil, nil
		}
		if create {
			return parent.CreateBucketIfNotExists([]byte(metaBucketName))
		}
		return parent.Bucket([]byte(metaBucketName)), nil
	}

	if create {
		return tx.CreateBucketIfNotExists([]byte(metaBucketName))
	}
	return tx.Bucket([]byte(metaBucketName)), nil
}
