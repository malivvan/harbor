package ts

import (
	"context"
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

	return se.store.b.Update(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		meta, err := se.store.readMeta(tx, se.name)
		if err != nil {
			return err
		}
		meta.Retention = r
		if err := se.store.writeMeta(tx, se.name, meta); err != nil {
			return err
		}
		return checkContext(ctx)
	})
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
	err := se.store.b.View(func(tx *bolt.Tx) error {
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
	err := se.store.b.Update(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		bucket := se.store.seriesBucket(tx, se.name)
		if bucket == nil {
			return nil
		}
		meta, err := se.store.readMeta(tx, se.name)
		if err != nil {
			return err
		}
		removed, err = se.store.pruneSeriesBucket(bucket, meta.Retention, now)
		if err != nil {
			return err
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
	err := s.b.Update(func(tx *bolt.Tx) error {
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

		return metaBucket.ForEach(func(physical, raw []byte) error {
			if err := checkContext(ctx); err != nil {
				return err
			}
			if string(physical) == markerKey {
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
			bucket := s.seriesBucket(tx, name)
			if bucket == nil {
				return nil
			}
			removed, err := s.pruneSeriesBucket(bucket, meta.Retention, now)
			if err != nil {
				return err
			}
			total += removed
			return checkContext(ctx)
		})
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}

// SetNow overrides the clock used for automatic retention enforcement during
// writes. Passing nil restores the default wall clock. Configure before
// concurrent use of the store.
func (s *Store) SetNow(fn func() int64) {
	if s == nil {
		return
	}
	if fn == nil {
		fn = defaultNow
	}
	s.now = fn
}

func (s *Store) nowNanos() int64 {
	if s != nil && s.now != nil {
		return s.now()
	}
	return defaultNow()
}

func defaultNow() int64 {
	return time.Now().UnixNano()
}

// pruneSeriesBucket applies a retention policy to a series. Plaintext and
// value-encrypted stores prune by timestamp key; block mode overrides this.
func (s *Store) pruneSeriesBucket(bucket *bolt.Bucket, r Retention, now int64) (int, error) {
	if s.blockDuration() > 0 {
		return s.pruneBlocks(bucket, r, now)
	}
	return pruneBucket(bucket, r, now), nil
}

func pruneBucket(b *bolt.Bucket, r Retention, now int64) int {
	cursor := b.Cursor()
	total := 0
	for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
		total++
	}
	if total == 0 || (r.MaxAge <= 0 && r.MaxPoints <= 0) {
		return 0
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
			age++
		}
		if age > remove {
			remove = age
		}
	}

	if remove <= 0 {
		return 0
	}

	cursor = b.Cursor()
	deleted := 0
	for key, _ := cursor.First(); key != nil && deleted < remove; key, _ = cursor.Next() {
		if err := b.Delete(key); err != nil {
			return deleted
		}
		deleted++
	}
	return deleted
}

func retentionCutoff(now, maxAge int64) int64 {
	if maxAge > 0 && now < math.MinInt64+maxAge {
		return math.MinInt64
	}
	return now - maxAge
}
