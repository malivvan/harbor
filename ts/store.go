package ts

import (
	"context"
	"sync"
	"sync/atomic"

	bolt "go.etcd.io/bbolt"
)

const Name = "ts"

const metaBucketName = "__ts_meta__"

// nowProvider wraps the pluggable clock so it can be stored in an atomic
// pointer.
type nowProvider struct {
	fn func() int64
}

// Store manages named time series backed by a bbolt database.
type Store struct {
	b      *bolt.DB
	root   [][]byte
	now    atomic.Pointer[nowProvider]
	mu     sync.RWMutex
	series map[string]*Series
	order  []string

	// enc is the optional encryption state. nil means plaintext.
	enc *encState
}

// encryptsValues reports whether values and series metadata are sealed.
func (s *Store) encryptsValues() bool {
	return s != nil && s.enc != nil && s.enc.encValues
}

// blockDuration returns the configured encryption time-block width, or zero
// when block mode is off.
func (s *Store) blockDuration() int64 {
	if s == nil || s.enc == nil {
		return 0
	}
	return s.enc.blockDur
}

// Root returns a copy of the store's root bucket path.
func (s *Store) Root() [][]byte {
	if s == nil {
		return nil
	}
	return cloneRoot(s.root)
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
			parent = s.rootBucket(tx)
		}

		key, err := s.bucketKey(name)
		if err != nil {
			return err
		}
		if parent == nil {
			if tx.Bucket(key) != nil {
				if err := tx.DeleteBucket(key); err != nil {
					return err
				}
				found = true
			}
		} else if parent.Bucket(key) != nil {
			if err := parent.DeleteBucket(key); err != nil {
				return err
			}
			found = true
		}

		meta, err := s.metaBucket(tx, false)
		if err != nil {
			return err
		}
		metaKey, err := s.bucketKey(name)
		if err != nil {
			return err
		}
		if meta != nil && meta.Get(metaKey) != nil {
			if err := meta.Delete(metaKey); err != nil {
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

// bucketKey returns the physical key for a series bucket and its metadata.
// Names are encrypted with deterministic AES-SIV when EncryptSeriesNames is on.
func (s *Store) bucketKey(name string) ([]byte, error) {
	if s == nil || s.enc == nil || !s.enc.encNames || s.enc.siv == nil {
		return []byte(name), nil
	}
	return s.enc.siv.SealName([]byte(name))
}

// logicalName maps a physical bucket/meta key back to the series name.
func (s *Store) logicalName(physical []byte) (string, error) {
	if s == nil || s.enc == nil || !s.enc.encNames || s.enc.siv == nil {
		return string(physical), nil
	}
	name, err := s.enc.siv.OpenName(physical)
	if err != nil {
		return "", err
	}
	return string(name), nil
}

// rootBucket traverses the configured root path, or returns nil when the path
// is empty or any element is missing.
func (s *Store) rootBucket(tx *bolt.Tx) *bolt.Bucket {
	var b *bolt.Bucket
	for _, name := range s.root {
		if len(name) == 0 {
			continue
		}
		if b == nil {
			b = tx.Bucket(name)
		} else {
			b = b.Bucket(name)
		}
		if b == nil {
			return nil
		}
	}
	return b
}

// createRootBucket creates the configured root path and returns its innermost
// bucket. An empty path returns nil.
func (s *Store) createRootBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	var b *bolt.Bucket
	for _, name := range s.root {
		if len(name) == 0 {
			continue
		}
		var err error
		if b == nil {
			b, err = tx.CreateBucketIfNotExists(name)
		} else {
			b, err = b.CreateBucketIfNotExists(name)
		}
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

// cloneRoot returns a deep copy of a bucket path.
func cloneRoot(root [][]byte) [][]byte {
	if root == nil {
		return nil
	}
	out := make([][]byte, len(root))
	for i, part := range root {
		out[i] = append([]byte(nil), part...)
	}
	return out
}

func (s *Store) seriesBucket(tx *bolt.Tx, name string) (*bolt.Bucket, error) {
	key, err := s.bucketKey(name)
	if err != nil {
		return nil, err
	}
	if len(s.root) == 0 {
		return tx.Bucket(key), nil
	}

	root := s.rootBucket(tx)
	if root == nil {
		return nil, nil
	}
	return root.Bucket(key), nil
}

func (s *Store) createSeriesBucket(tx *bolt.Tx, name string) (*bolt.Bucket, error) {
	key, err := s.bucketKey(name)
	if err != nil {
		return nil, err
	}
	if len(s.root) == 0 {
		return tx.CreateBucketIfNotExists(key)
	}

	root, err := s.createRootBucket(tx)
	if err != nil {
		return nil, err
	}
	return root.CreateBucketIfNotExists(key)
}

func (s *Store) metaBucket(tx *bolt.Tx, create bool) (*bolt.Bucket, error) {
	var parent *bolt.Bucket
	var err error

	if len(s.root) > 0 {
		if create {
			parent, err = s.createRootBucket(tx)
		} else {
			parent = s.rootBucket(tx)
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
