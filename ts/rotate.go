package ts

import (
	"context"
	"fmt"
	"sort"

	"github.com/malivvan/storm/internal/crypto"
	bolt "go.etcd.io/bbolt"
)

// withRotationKey returns the view a single-series rotation moves onto: the
// previous key stays available for decryption, so the series that were not
// rewritten keep working until RotateAll.
func (s *Store) withRotationKey(newKey []byte) (*Store, error) {
	cur := s.encState()
	if cur == nil {
		return nil, fmt.Errorf("%w: store is not encrypted", ErrInvalidConfig)
	}

	kc, err := cur.kc.WithKeyKeepingFallbacks(newKey)
	if err != nil {
		return nil, err
	}
	next := &Store{
		b:    s.b,
		root: cloneRoot(s.root),
	}
	next.enc.Store(&encState{
		kc:        kc,
		encValues: cur.encValues,
		blockDur:  cur.blockDur,
	})
	next.now.Store(s.now.Load())
	return next, nil
}

// withKey returns a store view that writes with newKey while sharing the same
// database, dropping every current key. It is used internally to re-encrypt
// data.
func (s *Store) withKey(newKey []byte) (*Store, error) {
	cur := s.encState()
	if cur == nil {
		return nil, fmt.Errorf("%w: store is not encrypted", ErrInvalidConfig)
	}
	if len(newKey) != KeySize {
		return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, KeySize, len(newKey))
	}

	kc, err := cur.kc.WithKey(newKey)
	if err != nil {
		return nil, err
	}

	next := &Store{
		b:    s.b,
		root: cloneRoot(s.root),
	}
	nextEnc := &encState{
		kc:        kc,
		encValues: cur.encValues,
		encNames:  cur.encNames,
		blockDur:  cur.blockDur,
	}
	if cur.encNames {
		nextEnc.siv, err = crypto.NewSIVNameCipher(newKey, sivInfo)
		if err != nil {
			return nil, err
		}
	}
	next.enc.Store(nextEnc)
	next.now.Store(s.now.Load())
	return next, nil
}

// adopt switches the store's write and read keys to next after a successful
// rotation. Readers load the pointer atomically, so they never observe a
// half-swapped key set.
func (s *Store) adopt(next *Store) {
	s.enc.Store(next.encState())
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
		if s.blockDuration() > 0 {
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
	root := s.rootBucket(tx)
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
	raw, err := s.encState().encodeMarker()
	if err != nil {
		return err
	}
	return bucket.Put([]byte(markerKey), raw)
}

// reEncryptSeries rewrites one series with next's key in its own transaction.
// The marker intentionally stays under the current key: the other series still
// hold old-key ciphertext, and a restart with the old key must be able to read
// them. Run RotateAll to finish the migration.
func (s *Store) reEncryptSeries(ctx context.Context, name string, next *Store) error {
	old := s.encState()
	err := s.b.Update(func(tx *bolt.Tx) error {
		if err := s.reEncryptSeriesTx(ctx, tx, name, next); err != nil {
			return err
		}
		if err := s.writeMarker(tx); err != nil {
			return err
		}
		// Adopt under the write lock: a writer that acquires the lock after
		// this transaction commits must see the new key.
		s.adopt(next)
		return nil
	})
	if err != nil {
		s.enc.Store(old)
		return err
	}
	return nil
}

// reEncryptSeriesTx rewrites one series with next's key inside tx, using s for
// reads.
func (s *Store) reEncryptSeriesTx(ctx context.Context, tx *bolt.Tx, name string, next *Store) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	meta, err := s.readMeta(tx, name)
	if err != nil {
		return err
	}

	oldBucket, err := s.seriesBucket(tx, name)
	if err != nil {
		return err
	}
	points, err := s.readAllPoints(ctx, oldBucket)
	if err != nil {
		return err
	}

	metaBucket, err := s.metaBucket(tx, false)
	if err != nil {
		return err
	}
	oldMetaKey, err := s.bucketKey(name)
	if err != nil {
		return err
	}
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
		if next.blockDuration() > 0 {
			if _, err := next.insertBlocks(ctx, bucket, points); err != nil {
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

	return nil
}

// ReEncryptSeries rewrites every record and the metadata of one series under
// newKey and installs newKey as the write key.
//
// The marker stays under the current key, and the previous key stays available
// for decryption in this process, so the series that were not rewritten keep
// working. This is a partial rotation: the store is NOT self-consistent for a
// fresh process until RotateAll completes it. Closing a store after
// ReEncryptSeries is unsupported; call RotateAll before Close.
//
// With EncryptSeriesNames it cannot run at all: the physical names are derived
// from one master key, so once the new key is installed the remaining series can
// neither be found nor decrypted. Those stores must be rotated with RotateAll,
// and ReEncryptSeries reports ErrInvalidConfig instead of leaving the database
// unrecoverable.
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
	if enc := s.encState(); enc != nil && enc.encNames {
		return fmt.Errorf("%w: a single series cannot be rotated when series names are encrypted; use RotateAll", ErrInvalidConfig)
	}

	next, err := s.withRotationKey(newKey)
	if err != nil {
		return err
	}
	return s.reEncryptSeries(ctx, name, next)
}

// RotateAll re-encrypts every known series under newKey, adopts it and drops
// the previous key.
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

	// Rotate every series and publish the new marker in one transaction, so a
	// crash or cancellation leaves the whole store under the old key. The key
	// state is adopted inside the transaction for the same reason: a writer
	// that acquires the bbolt write lock after the commit sees the new key.
	//
	// The series list is enumerated inside that same transaction. Collecting
	// it in an earlier read transaction left a window in which a writer could
	// create a series that the rewrite never visited; its points would stay
	// sealed under the old key, which withKey then drops, making the series
	// permanently undecryptable.
	old := s.encState()
	if err := s.b.Update(func(tx *bolt.Tx) error {
		names, err := s.allSeriesNames(tx)
		if err != nil {
			return err
		}
		for _, name := range names {
			if err := checkContext(ctx); err != nil {
				return err
			}
			if err := s.reEncryptSeriesTx(ctx, tx, name, next); err != nil {
				return err
			}
		}
		if err := next.writeMarker(tx); err != nil {
			return err
		}
		s.adopt(next)
		return nil
	}); err != nil {
		s.enc.Store(old)
		return err
	}
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
		if enc := s.encState(); enc != nil {
			meta, err := s.metaBucket(tx, false)
			if err != nil {
				return err
			}
			if meta != nil {
				if raw := meta.Get([]byte(markerKey)); raw != nil {
					m, canary, err := decodeMarker(raw)
					if err != nil {
						return err
					}
					if err := enc.verifyCanary(canary, m); err != nil {
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
			bucket, err := s.seriesBucket(tx, name)
			if err != nil {
				return err
			}
			_, err = s.readAllPoints(ctx, bucket)
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
		if root := s.rootBucket(tx); root != nil {
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
			if string(key) == markerKey || string(key) == versionKey {
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
	cached := append([]string(nil), s.order...)
	s.mu.RUnlock()
	for _, name := range cached {
		// A series that was only handed out by Store.Series and never written
		// has no bucket or metadata; including it would make RotateAll fail.
		bucket, err := s.seriesBucket(tx, name)
		if err != nil {
			return nil, err
		}
		if bucket != nil {
			add(name)
			continue
		}
		if meta != nil {
			key, err := s.bucketKey(name)
			if err != nil {
				return nil, err
			}
			if meta.Get(key) != nil {
				add(name)
			}
		}
	}

	sort.Strings(names)
	return names, nil
}
