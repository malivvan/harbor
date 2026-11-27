package series

import (
	"context"
	"encoding/json"
	"math"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Retention describes how long points are kept in a series.
type Retention struct {
	// MaxAge drops points older than the reference time minus MaxAge.
	// 0 disables age-based retention.
	MaxAge time.Duration
	// MaxPoints keeps at most the most-recent MaxPoints points.
	// 0 disables count-based retention.
	MaxPoints int
}

// satisfied reports whether the policy is already met, so a write can skip
// scanning or decrypting the series.
func (r Retention) satisfied(count int64) bool {
	return r.MaxAge <= 0 && (r.MaxPoints <= 0 || count <= int64(r.MaxPoints))
}

func (r Retention) validate() error {
	if r.MaxAge < 0 || r.MaxPoints < 0 {
		return ErrInvalidRetention
	}
	return nil
}

// SetRetention persists the series retention policy. The zero value clears it.
func (se *Series) SetRetention(ctx context.Context, r Retention) error {
	if se == nil || se.store == nil {
		return ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := r.validate(); err != nil {
		return err
	}

	if err := se.store.update(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		meta, err := se.store.readMeta(tx, se.name)
		if err != nil {
			return err
		}
		meta.Retention = r

		// A series that predates count tracking has Count == 0 even though it
		// holds points. Seed it before enforcing a count-based policy,
		// otherwise the next insert would under-count and skip pruning.
		if meta.Count == 0 && (r.MaxPoints > 0 || r.MaxAge > 0) {
			count, err := se.store.seriesCount(ctx, tx, se.name)
			if err != nil {
				return err
			}
			meta.Count = count
		}

		if err := se.store.writeMeta(tx, se.name, meta); err != nil {
			return err
		}
		return checkContext(ctx)
	}); err != nil {
		return err
	}
	raw, _ := json.Marshal(r)
	return se.store.traceEntry("set_retention", se.name, digestOf(raw))
}

// Retention returns the persisted policy; the zero value when unset.
func (se *Series) Retention(ctx context.Context) (Retention, error) {
	if se == nil || se.store == nil {
		return Retention{}, ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return Retention{}, err
	}

	var retention Retention
	err := se.store.view(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}
		meta, err := se.store.readMeta(tx, se.name)
		if err != nil {
			return err
		}
		retention = meta.Retention
		return checkContext(ctx)
	})
	if err != nil {
		return Retention{}, err
	}
	return retention, nil
}

// EnforceRetention prunes points that violate the persisted policy, using now
// as the reference time for MaxAge. A missing series yields 0, nil.
func (se *Series) EnforceRetention(ctx context.Context, now int64) (int, error) {
	if se == nil || se.store == nil {
		return 0, ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return 0, err
	}

	removed := 0
	err := se.store.update(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		bucket, err := se.store.seriesBucket(tx, se.name)
		if err != nil {
			return err
		}
		if bucket == nil {
			return nil
		}
		meta, err := se.store.readMeta(tx, se.name)
		if err != nil {
			return err
		}
		removed, err = se.store.pruneSeriesBucket(ctx, bucket, meta.Retention, now)
		if err != nil {
			return err
		}
		if removed > 0 {
			// Persist the pruned count, mirroring the store-wide variant.
			// Otherwise Count() keeps over-reporting and every write re-prunes.
			meta.Count -= int64(removed)
			if meta.Count < 0 {
				meta.Count = 0
			}
			if err := se.store.writeMeta(tx, se.name, meta); err != nil {
				return err
			}
		}
		return checkContext(ctx)
	})
	if err != nil {
		return 0, err
	}
	return removed, nil
}

// EnforceRetention enforces the retention policy of every persisted series that
// has one, using now as the reference time. It returns the total points removed.
func (s *Store) EnforceRetention(ctx context.Context, now int64) (int, error) {
	if s == nil || s.b == nil {
		return 0, ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return 0, err
	}

	total := 0
	type metaUpdate struct {
		name string
		meta seriesMeta
	}
	var updates []metaUpdate
	err := s.update(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		metaBucket, err := s.metaBucket(tx, false)
		if err != nil {
			return err
		}
		if metaBucket == nil {
			return nil
		}

		walkErr := metaBucket.ForEach(func(physical, raw []byte) error {
			if err := checkContext(ctx); err != nil {
				return err
			}
			if string(physical) == markerKey || string(physical) == versionKey {
				return nil
			}

			meta, err := s.decodeMeta(raw)
			if err != nil {
				return err
			}
			name, err := s.logicalName(physical)
			if err != nil {
				return err
			}
			bucket, err := s.seriesBucket(tx, name)
			if err != nil {
				return err
			}
			if bucket == nil {
				return nil
			}
			removed, err := s.pruneSeriesBucket(ctx, bucket, meta.Retention, now)
			if err != nil {
				return err
			}
			if removed > 0 {
				meta.Count -= int64(removed)
				if meta.Count < 0 {
					meta.Count = 0
				}
				updates = append(updates, metaUpdate{name: name, meta: meta})
			}
			total += removed
			return checkContext(ctx)
		})
		if walkErr != nil {
			return walkErr
		}
		for _, update := range updates {
			if err := s.writeMeta(tx, update.name, update.meta); err != nil {
				return err
			}
		}
		return checkContext(ctx)
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}

// SetNow overrides the clock used for automatic retention enforcement during
// writes. Passing nil restores the default wall clock. SetNow is safe to call
// concurrently with other Store operations.
func (s *Store) SetNow(fn func() int64) {
	if s == nil {
		return
	}
	if fn == nil {
		fn = defaultNow
	}
	s.rootStore().now.Store(&nowProvider{fn: fn})
}

func (s *Store) nowNanos() int64 {
	if s == nil {
		return defaultNow()
	}
	if p := s.nowOf(); p != nil {
		return p.fn()
	}
	return defaultNow()
}

// storeNow is the retention reference time for a non-replicated write: the
// store's current clock. A replicated writer instead pins the proposal-time
// clock in the command and passes it to InsertAt/InsertManyAt, so every node
// prunes with the same reference time.
func (se *Series) storeNow() int64 {
	if se == nil || se.store == nil {
		return defaultNow()
	}
	return se.store.nowNanos()
}

func defaultNow() int64 {
	return time.Now().UnixNano()
}

// pruneSeriesBucket applies a retention policy to a series. Plaintext and
// value-encrypted stores prune by timestamp key; block mode overrides this. A
// zero policy returns before touching the bucket, so an insert never scans (or
// decrypts) the existing points when no retention is configured.
func (s *Store) pruneSeriesBucket(ctx context.Context, bucket *bolt.Bucket, r Retention, now int64) (int, error) {
	if r.MaxAge <= 0 && r.MaxPoints <= 0 {
		return 0, nil
	}
	if s.blockDuration() > 0 {
		return s.pruneBlocks(ctx, bucket, r, now)
	}
	return pruneBucket(ctx, bucket, r, now)
}

func pruneBucket(ctx context.Context, b *bolt.Bucket, r Retention, now int64) (int, error) {
	if r.MaxAge <= 0 && r.MaxPoints <= 0 {
		return 0, nil
	}

	cursor := b.Cursor()
	total := 0
	for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
		if err := checkContext(ctx); err != nil {
			return 0, err
		}
		total++
	}
	if total == 0 {
		return 0, nil
	}

	remove := 0
	if r.MaxPoints > 0 && total > r.MaxPoints {
		remove = total - r.MaxPoints
	}

	if r.MaxAge > 0 {
		cutoff := retentionCutoff(now, int64(r.MaxAge))
		cursor = b.Cursor()
		age := 0
		for key, _ := cursor.First(); key != nil && decodeKey(key) < cutoff; key, _ = cursor.Next() {
			if err := checkContext(ctx); err != nil {
				return 0, err
			}
			age++
		}
		if age > remove {
			remove = age
		}
	}

	if remove <= 0 {
		return 0, nil
	}

	// Collect the keys to drop first, then delete them after the cursor loop so
	// the traversal never observes a bucket mutated underneath it.
	cursor = b.Cursor()
	keys := make([][]byte, 0, remove)
	for key, _ := cursor.First(); key != nil && len(keys) < remove; key, _ = cursor.Next() {
		if err := checkContext(ctx); err != nil {
			return 0, err
		}
		keys = append(keys, append([]byte(nil), key...))
	}

	deleted := 0
	for _, key := range keys {
		if err := checkContext(ctx); err != nil {
			return deleted, err
		}
		if err := b.Delete(key); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func retentionCutoff(now, maxAge int64) int64 {
	if maxAge > 0 && now < math.MinInt64+maxAge {
		return math.MinInt64
	}
	return now - maxAge
}
