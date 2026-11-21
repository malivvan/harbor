package ts

import (
	"context"
	"fmt"
	"sort"

	"github.com/malivvan/storm/internal/encryption"
	bolt "go.etcd.io/bbolt"
)

// MultiKeyProvider supplies several keys for rotation: the first is used for
// new writes, and every key is tried when decrypting existing data.
type MultiKeyProvider struct {
	providers []KeyProvider
}

// NewMultiKeyProvider creates a multi-key provider. At least one provider is
// required; the first one is the primary key.
func NewMultiKeyProvider(providers ...KeyProvider) (*MultiKeyProvider, error) {
	if len(providers) == 0 {
		return nil, fmt.Errorf("%w: at least one key provider required", ErrInvalidConfig)
	}
	return &MultiKeyProvider{providers: providers}, nil
}

// Key returns the primary key.
func (m *MultiKeyProvider) Key() ([]byte, error) {
	return m.providers[0].Key()
}

// TryKeys returns every configured key, primary first.
func (m *MultiKeyProvider) TryKeys() ([][]byte, error) {
	keys := make([][]byte, 0, len(m.providers))
	for _, provider := range m.providers {
		key, err := provider.Key()
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// withKey returns a store view that writes with newKey while sharing the same
// database. It is used internally to re-encrypt data.
func (s *Store) withKey(newKey []byte) (*Store, error) {
	if s.engine == nil {
		return nil, fmt.Errorf("%w: store is not encrypted", ErrInvalidConfig)
	}
	if len(newKey) != KeySize {
		return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, KeySize, len(newKey))
	}

	engine, err := encryption.NewCipherEngine(s.cipher, newKey)
	if err != nil {
		return nil, err
	}

	next := &Store{
		b:         s.b,
		root:      s.root,
		now:       s.now,
		engine:    engine,
		engines:   []CipherEngine{engine},
		cipher:    s.cipher,
		encValues: s.encValues,
		encNames:  s.encNames,
		blockDur:  s.blockDur,
	}
	if s.encNames {
		next.siv, err = newSIVEngine(newKey)
		if err != nil {
			return nil, err
		}
	}
	return next, nil
}

// adopt switches the store's write and read keys to next after a successful
// rotation.
func (s *Store) adopt(next *Store) {
	s.engine = next.engine
	s.engines = next.engines
	s.siv = next.siv
}

// readAllPoints returns every point stored in a series bucket.
func (s *Store) readAllPoints(ctx context.Context, bucket *bolt.Bucket) ([]Point, error) {
	if bucket == nil {
		return nil, nil
	}

	var points []Point
	cursor := bucket.Cursor()
	for key, raw := cursor.First(); key != nil; key, raw = cursor.Next() {
		if err := checkContext(ctx); err != nil {
			return nil, err
		}
		if len(key) != recordSize {
			continue
		}
		if s.blockDur > 0 {
			block, err := s.openBlock(raw)
			if err != nil {
				return nil, err
			}
			points = append(points, block...)
			continue
		}
		point, ok, err := s.decodePoint(key, raw)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		points = append(points, point)
	}
	return points, nil
}

// deleteBucket removes a series bucket identified by its physical key.
func (s *Store) deleteBucket(tx *bolt.Tx, key []byte) error {
	if len(s.root) == 0 {
		if tx.Bucket(key) != nil {
			return tx.DeleteBucket(key)
		}
		return nil
	}
	root := tx.Bucket(s.root)
	if root != nil && root.Bucket(key) != nil {
		return root.DeleteBucket(key)
	}
	return nil
}

// writeMarker stores (or overwrites) the encryption marker.
func (s *Store) writeMarker(tx *bolt.Tx) error {
	bucket, err := s.metaBucket(tx, true)
	if err != nil {
		return err
	}
	raw, err := s.encodeMarker()
	if err != nil {
		return err
	}
	return bucket.Put([]byte(markerKey), raw)
}

// reEncryptSeries rewrites one series with next's key, using s for reads.
func (s *Store) reEncryptSeries(ctx context.Context, name string, next *Store) error {
	return s.b.Update(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		meta, err := s.readMeta(tx, name)
		if err != nil {
			return err
		}

		oldBucket := s.seriesBucket(tx, name)
		points, err := s.readAllPoints(ctx, oldBucket)
		if err != nil {
			return err
		}

		metaBucket, err := s.metaBucket(tx, false)
		if err != nil {
			return err
		}
		oldMetaKey := s.bucketKey(name)
		hasMeta := metaBucket != nil && metaBucket.Get(oldMetaKey) != nil
		if oldBucket == nil && !hasMeta {
			return ErrNotFound
		}

		if oldBucket != nil {
			if err := s.deleteBucket(tx, oldMetaKey); err != nil {
				return err
			}
		}
		if hasMeta {
			if err := metaBucket.Delete(oldMetaKey); err != nil {
				return err
			}
		}

		if len(points) > 0 {
			bucket, err := next.createSeriesBucket(tx, name)
			if err != nil {
				return err
			}
			if next.blockDur > 0 {
				if err := next.insertBlocks(ctx, bucket, points); err != nil {
					return err
				}
			} else {
				for _, point := range points {
					if err := checkContext(ctx); err != nil {
						return err
					}
					key := encodeKey(point.Timestamp)
					value, err := next.sealValue(point.Value)
					if err != nil {
						return err
					}
					if err := bucket.Put(key[:], value); err != nil {
						return err
					}
				}
			}
		}

		if hasMeta {
			if err := next.writeMeta(tx, name, meta); err != nil {
				return err
			}
		}

		return next.writeMarker(tx)
	})
}

// ReEncryptSeries rewrites every record and the metadata of one series under
// newKey. The store adopts the new key on success.
func (s *Store) ReEncryptSeries(ctx context.Context, name string, newKey []byte) error {
	if s == nil || s.b == nil {
		return ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if name == "" {
		return ErrNilParam
	}

	next, err := s.withKey(newKey)
	if err != nil {
		return err
	}
	if err := s.reEncryptSeries(ctx, name, next); err != nil {
		return err
	}
	s.adopt(next)
	return nil
}

// RotateAll re-encrypts every known series under newKey and adopts it.
func (s *Store) RotateAll(ctx context.Context, newKey []byte) error {
	if s == nil || s.b == nil {
		return ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return err
	}

	next, err := s.withKey(newKey)
	if err != nil {
		return err
	}

	var names []string
	if err := s.b.View(func(tx *bolt.Tx) error {
		var err error
		names, err = s.allSeriesNames(tx)
		return err
	}); err != nil {
		return err
	}

	for _, name := range names {
		if err := checkContext(ctx); err != nil {
			return err
		}
		if err := s.reEncryptSeries(ctx, name, next); err != nil {
			return err
		}
	}

	s.adopt(next)
	return nil
}

// VerifyAll re-reads the marker, metadata and points of every known series to
// confirm they decrypt with the configured keys.
func (s *Store) VerifyAll(ctx context.Context) error {
	if s == nil || s.b == nil {
		return ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return err
	}

	var names []string
	if err := s.b.View(func(tx *bolt.Tx) error {
		if s.engine != nil {
			meta, err := s.metaBucket(tx, false)
			if err != nil {
				return err
			}
			if meta != nil {
				if raw := meta.Get([]byte(markerKey)); raw != nil {
					_, canary, err := decodeMarker(raw)
					if err != nil {
						return err
					}
					if err := s.verifyCanary(canary); err != nil {
						return err
					}
				}
			}
		}
		var err error
		names, err = s.allSeriesNames(tx)
		return err
	}); err != nil {
		return err
	}

	for _, name := range names {
		if err := checkContext(ctx); err != nil {
			return err
		}
		if err := s.b.View(func(tx *bolt.Tx) error {
			if _, err := s.readMeta(tx, name); err != nil {
				return err
			}
			_, err := s.readAllPoints(ctx, s.seriesBucket(tx, name))
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

// allSeriesNames collects logical series names from the root buckets, the
// metadata bucket and the in-memory cache.
func (s *Store) allSeriesNames(tx *bolt.Tx) ([]string, error) {
	seen := make(map[string]bool)
	var names []string
	add := func(name string) {
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		names = append(names, name)
	}

	if len(s.root) > 0 {
		if root := tx.Bucket(s.root); root != nil {
			if err := root.ForEach(func(key, value []byte) error {
				if value != nil || string(key) == metaBucketName {
					return nil
				}
				name, err := s.logicalName(key)
				if err != nil {
					return err
				}
				add(name)
				return nil
			}); err != nil {
				return nil, err
			}
		}
	}

	meta, err := s.metaBucket(tx, false)
	if err != nil {
		return nil, err
	}
	if meta != nil {
		if err := meta.ForEach(func(key, _ []byte) error {
			if string(key) == markerKey {
				return nil
			}
			name, err := s.logicalName(key)
			if err != nil {
				return err
			}
			add(name)
			return nil
		}); err != nil {
			return nil, err
		}
	}

	s.mu.RLock()
	for _, name := range s.order {
		add(name)
	}
	s.mu.RUnlock()

	sort.Strings(names)
	return names, nil
}
