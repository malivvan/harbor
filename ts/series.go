package ts

import (
	"context"
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// Point is a single timestamped sample.
type Point struct {
	Timestamp int64   // Unix time in nanoseconds
	Value     float64 // sample value
}

// Sample is one aggregated value over a time bucket.
type Sample struct {
	Timestamp int64   // start of the bucket (Unix nanoseconds)
	Value     float64 // aggregate result
}

// Series is a named, ordered sequence of timestamped points.
type Series struct {
	store *Store
	name  string
}

type seriesMeta struct {
	Labels    map[string]string `json:"labels,omitempty"`
	Retention Retention         `json:"retention,omitempty"`
	// Count is the number of live points. It lets Count avoid decrypting every
	// block and lets a satisfied retention policy skip pruning entirely.
	Count int64 `json:"count,omitempty"`
}

func (s *Store) readMeta(tx *bolt.Tx, name string) (seriesMeta, error) {
	var meta seriesMeta
	bucket, err := s.metaBucket(tx, false)
	if err != nil {
		return meta, err
	}
	if bucket == nil {
		return meta, nil
	}
	key, err := s.bucketKey(name)
	if err != nil {
		return meta, err
	}
	raw := bucket.Get(key)
	if raw == nil {
		return meta, nil
	}
	return s.decodeMeta(raw)
}

// decodeMeta unmarshals series metadata, decrypting it when value encryption is
// enabled.
func (s *Store) decodeMeta(raw []byte) (seriesMeta, error) {
	var meta seriesMeta
	if s.encryptsValues() {
		plain, err := s.openBytes(raw)
		if err != nil {
			return seriesMeta{}, err
		}
		raw = plain
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		return seriesMeta{}, err
	}
	return meta, nil
}

func (s *Store) writeMeta(tx *bolt.Tx, name string, meta seriesMeta) error {
	if err := s.ensureMarker(tx); err != nil {
		return err
	}
	bucket, err := s.metaBucket(tx, true)
	if err != nil {
		return err
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if s.encryptsValues() {
		data, err = s.sealBytes(data)
		if err != nil {
			return err
		}
	}
	key, err := s.bucketKey(name)
	if err != nil {
		return err
	}
	return bucket.Put(key, data)
}

// Name returns the series name.
func (se *Series) Name() string {
	if se == nil {
		return ""
	}
	return se.name
}

// Store returns the Store that owns the series.
func (se *Series) Store() *Store {
	if se == nil {
		return nil
	}
	return se.store
}

// Labels returns the series labels. An unset labels map is returned as an empty
// map.
func (se *Series) Labels(ctx context.Context) (map[string]string, error) {
	if se == nil || se.store == nil {
		return nil, ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	labels := make(map[string]string)
	err := se.store.b.View(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		stored, err := se.store.readMeta(tx, se.name)
		if err != nil {
			return err
		}
		for key, value := range stored.Labels {
			labels[key] = value
		}
		return checkContext(ctx)
	})
	if err != nil {
		return nil, err
	}
	return labels, nil
}

// SetLabels replaces the series labels.
func (se *Series) SetLabels(ctx context.Context, labels map[string]string) error {
	if se == nil || se.store == nil {
		return ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if labels == nil {
		labels = make(map[string]string)
	}

	return se.store.b.Update(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}
		meta, err := se.store.readMeta(tx, se.name)
		if err != nil {
			return err
		}
		meta.Labels = labels
		// Seed the count for a series that predates count tracking, so a
		// later insert does not start from a stale zero.
		if meta.Count == 0 {
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
	})
}

// Insert stores one point, replacing any point with the same timestamp.
// It avoids the one-element slice and group map that InsertMany needs.
func (se *Series) Insert(ctx context.Context, p Point) error {
	if se == nil || se.store == nil {
		return ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return err
	}

	return se.store.b.Update(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}
		if err := se.store.ensureMarker(tx); err != nil {
			return err
		}
		bucket, err := se.store.createSeriesBucket(tx, se.name)
		if err != nil {
			return err
		}

		added := 0
		if se.store.blockDuration() > 0 {
			var err error
			added, err = se.store.insertBlockPoint(ctx, bucket, p)
			if err != nil {
				return err
			}
		} else {
			key := encodeKey(p.Timestamp)
			value, err := se.store.sealValue(p.Value)
			if err != nil {
				return err
			}
			if bucket.Get(key[:]) == nil {
				added = 1
			}
			if err := bucket.Put(key[:], value); err != nil {
				return err
			}
		}

		return se.updateMetaAfterInsert(ctx, tx, bucket, added)
	})
}

// InsertMany stores multiple points in a single transaction. Out-of-order
// timestamps are accepted; the last value for a timestamp wins.
func (se *Series) InsertMany(ctx context.Context, points []Point) error {
	if se == nil || se.store == nil {
		return ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if len(points) == 0 {
		return nil
	}

	return se.store.b.Update(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		if err := se.store.ensureMarker(tx); err != nil {
			return err
		}

		bucket, err := se.store.createSeriesBucket(tx, se.name)
		if err != nil {
			return err
		}

		added := 0
		if se.store.blockDuration() > 0 {
			var err error
			added, err = se.store.insertBlocks(ctx, bucket, points)
			if err != nil {
				return err
			}
		} else {
			for _, point := range points {
				if err := checkContext(ctx); err != nil {
					return err
				}
				key := encodeKey(point.Timestamp)
				value, err := se.store.sealValue(point.Value)
				if err != nil {
					return err
				}
				if bucket.Get(key[:]) == nil {
					added++
				}
				if err := bucket.Put(key[:], value); err != nil {
					return err
				}
			}
		}

		return se.updateMetaAfterInsert(ctx, tx, bucket, added)
	})
}

// updateMetaAfterInsert adjusts the persisted count and applies retention.
// Metadata is only rewritten when the count actually changed (or a legacy
// series first gains a retention policy), so streaming inserts do not decode
// and re-encode the metadata document on every call.
func (se *Series) updateMetaAfterInsert(ctx context.Context, tx *bolt.Tx, bucket *bolt.Bucket, added int) error {
	meta, err := se.store.readMeta(tx, se.name)
	if err != nil {
		return err
	}
	metaKey, err := se.store.bucketKey(se.name)
	if err != nil {
		return err
	}
	metaBucket, err := se.store.metaBucket(tx, false)
	if err != nil {
		return err
	}
	hadMeta := metaBucket != nil && metaBucket.Get(metaKey) != nil

	removed := 0
	meta.Count += int64(added)
	if !meta.Retention.satisfied(meta.Count) {
		removed, err = se.store.pruneSeriesBucket(ctx, bucket, meta.Retention, se.store.nowNanos())
		if err != nil {
			return err
		}
		meta.Count -= int64(removed)
	}
	if meta.Count < 0 {
		meta.Count = 0
	}

	// A series with no metadata and no retention policy must not gain a
	// metadata record just from an insert.
	if (hadMeta && (added > 0 || removed > 0)) || (!hadMeta && meta.Retention != (Retention{})) {
		if err := se.store.writeMeta(tx, se.name, meta); err != nil {
			return err
		}
	}

	return checkContext(ctx)
}

// Get returns the point exactly at ts, or ErrNotFound.
func (se *Series) Get(ctx context.Context, ts int64) (Point, error) {
	if se == nil || se.store == nil {
		return Point{}, ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return Point{}, err
	}

	var point Point
	err := se.store.b.View(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		bucket, err := se.store.seriesBucket(tx, se.name)
		if err != nil {
			return err
		}
		if bucket == nil {
			return ErrNotFound
		}
		if se.store.blockDuration() > 0 {
			points, ok, err := se.store.readBlock(bucket, blockStart(ts, se.store.blockDuration()))
			if err != nil {
				return err
			}
			if !ok {
				return ErrNotFound
			}
			for _, stored := range points {
				if stored.Timestamp == ts {
					point = stored
					return checkContext(ctx)
				}
			}
			return ErrNotFound
		}

		key := encodeKey(ts)
		raw := bucket.Get(key[:])
		if raw == nil {
			return ErrNotFound
		}
		decoded, ok, err := se.store.decodePoint(key[:], raw)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("ts: corrupt point at %d", ts)
		}
		point = decoded
		return checkContext(ctx)
	})
	if err != nil {
		return Point{}, err
	}
	return point, nil
}

// First returns the earliest point, or ErrNotFound when the series is empty.
func (se *Series) First(ctx context.Context) (Point, error) {
	if se == nil || se.store == nil {
		return Point{}, ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return Point{}, err
	}

	var point Point
	err := se.store.b.View(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		bucket, err := se.store.seriesBucket(tx, se.name)
		if err != nil {
			return err
		}
		if bucket == nil {
			return ErrNotFound
		}
		if se.store.blockDuration() > 0 {
			stored, ok, err := se.store.blockPoint(bucket, false)
			if err != nil {
				return err
			}
			if !ok {
				return ErrNotFound
			}
			point = stored
			return checkContext(ctx)
		}

		key, value := bucket.Cursor().First()
		if key == nil {
			return ErrNotFound
		}
		decoded, ok, err := se.store.decodePoint(key, value)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("ts: corrupt point")
		}
		point = decoded
		return checkContext(ctx)
	})
	if err != nil {
		return Point{}, err
	}
	return point, nil
}

// Last returns the latest point, or ErrNotFound when the series is empty.
func (se *Series) Last(ctx context.Context) (Point, error) {
	if se == nil || se.store == nil {
		return Point{}, ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return Point{}, err
	}

	var point Point
	err := se.store.b.View(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		bucket, err := se.store.seriesBucket(tx, se.name)
		if err != nil {
			return err
		}
		if bucket == nil {
			return ErrNotFound
		}
		if se.store.blockDuration() > 0 {
			stored, ok, err := se.store.blockPoint(bucket, true)
			if err != nil {
				return err
			}
			if !ok {
				return ErrNotFound
			}
			point = stored
			return checkContext(ctx)
		}

		key, value := bucket.Cursor().Last()
		if key == nil {
			return ErrNotFound
		}
		decoded, ok, err := se.store.decodePoint(key, value)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("ts: corrupt point")
		}
		point = decoded
		return checkContext(ctx)
	})
	if err != nil {
		return Point{}, err
	}
	return point, nil
}

// Range returns points with start <= Timestamp <= end, in time order.
func (se *Series) Range(ctx context.Context, start, end int64) ([]Point, error) {
	if se == nil || se.store == nil {
		return nil, ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if start > end {
		return nil, ErrInvalidRange
	}

	points := make([]Point, 0)
	err := se.store.b.View(func(tx *bolt.Tx) error {
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

		// Pre-size from the persisted count when available; otherwise the
		// bucket key count is a reasonable hint.
		hint := bucket.Stats().KeyN
		if se.store.blockDuration() > 0 {
			if meta, metaErr := se.store.readMeta(tx, se.name); metaErr == nil && meta.Count > 0 {
				hint = int(meta.Count)
			}
		}
		points = make([]Point, 0, hint)

		if se.store.blockDuration() > 0 {
			return se.store.scanBlocks(ctx, bucket, start, end, func(point Point) error {
				points = append(points, point)
				return nil
			})
		}

		startKey := encodeKey(start)
		cursor := bucket.Cursor()
		for key, value := cursor.Seek(startKey[:]); key != nil; key, value = cursor.Next() {
			if err := checkContext(ctx); err != nil {
				return err
			}
			point, ok, err := se.store.decodePoint(key, value)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			if point.Timestamp > end {
				break
			}
			points = append(points, point)
		}
		return checkContext(ctx)
	})
	if err != nil {
		return nil, err
	}
	return points, nil
}

// RangeEach streams points with start <= Timestamp <= end in time order,
// calling fn for each point. Iteration stops at the first fn error.
func (se *Series) RangeEach(ctx context.Context, start, end int64, fn func(Point) error) error {
	if se == nil || se.store == nil || fn == nil {
		return ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if start > end {
		return ErrInvalidRange
	}

	return se.store.b.View(func(tx *bolt.Tx) error {
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

		if se.store.blockDuration() > 0 {
			return se.store.scanBlocks(ctx, bucket, start, end, fn)
		}

		startKey := encodeKey(start)
		cursor := bucket.Cursor()
		for key, value := cursor.Seek(startKey[:]); key != nil; key, value = cursor.Next() {
			if err := checkContext(ctx); err != nil {
				return err
			}
			point, ok, err := se.store.decodePoint(key, value)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			if point.Timestamp > end {
				break
			}
			if err := fn(point); err != nil {
				return err
			}
		}
		return checkContext(ctx)
	})
}

// Count returns the number of points in the series.
func (se *Series) Count(ctx context.Context) (int, error) {
	if se == nil || se.store == nil {
		return 0, ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return 0, err
	}

	var (
		count   int
		persist bool
	)
	err := se.store.b.View(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}
		bucket, err := se.store.seriesBucket(tx, se.name)
		if err != nil {
			return err
		}
		if bucket == nil {
			return checkContext(ctx)
		}
		meta, err := se.store.readMeta(tx, se.name)
		if err != nil {
			return err
		}
		if meta.Count > 0 {
			count = int(meta.Count)
			return checkContext(ctx)
		}
		if se.store.blockDuration() > 0 {
			var blockErr error
			count, blockErr = se.store.countBlocks(ctx, bucket)
			// Legacy or crash-recovered series: persist the count once so
			// subsequent calls do not decrypt every block again.
			persist = blockErr == nil && count > 0
			return blockErr
		}
		count = bucket.Stats().KeyN
		return checkContext(ctx)
	})
	if err != nil {
		return 0, err
	}

	if persist {
		// Best effort: a failed cache write must not fail the count.
		_ = se.store.b.Update(func(tx *bolt.Tx) error {
			meta, err := se.store.readMeta(tx, se.name)
			if err != nil {
				return err
			}
			if meta.Count != 0 {
				return nil
			}
			meta.Count = int64(count)
			return se.store.writeMeta(tx, se.name, meta)
		})
	}
	return count, nil
}

// DeleteRange deletes points with start <= Timestamp <= end and returns the
// number deleted.
func (se *Series) DeleteRange(ctx context.Context, start, end int64) (int, error) {
	if se == nil || se.store == nil {
		return 0, ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return 0, err
	}
	if start > end {
		return 0, ErrInvalidRange
	}

	deleted := 0
	err := se.store.b.Update(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		if err := se.store.ensureMarker(tx); err != nil {
			return err
		}

		bucket, err := se.store.seriesBucket(tx, se.name)
		if err != nil {
			return err
		}
		if bucket == nil {
			return nil
		}

		if se.store.blockDuration() > 0 {
			var blockErr error
			deleted, blockErr = se.store.deleteBlocks(ctx, bucket, start, end)
			if blockErr != nil {
				return blockErr
			}
			meta, err := se.store.readMeta(tx, se.name)
			if err != nil {
				return err
			}
			meta.Count -= int64(deleted)
			if meta.Count < 0 {
				meta.Count = 0
			}
			return se.store.writeMeta(tx, se.name, meta)
		}

		var keys [][]byte
		startKey := encodeKey(start)
		cursor := bucket.Cursor()
		for key, value := cursor.Seek(startKey[:]); key != nil; key, value = cursor.Next() {
			if err := checkContext(ctx); err != nil {
				return err
			}
			point, ok, err := se.store.decodePoint(key, value)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			if point.Timestamp > end {
				break
			}
			keys = append(keys, append([]byte(nil), key...))
		}

		for _, key := range keys {
			if err := checkContext(ctx); err != nil {
				return err
			}
			if err := bucket.Delete(key); err != nil {
				return err
			}
			deleted++
		}
		meta, err := se.store.readMeta(tx, se.name)
		if err != nil {
			return err
		}
		meta.Count -= int64(deleted)
		if meta.Count < 0 {
			meta.Count = 0
		}
		return se.store.writeMeta(tx, se.name, meta)
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

// Drop removes the series and its metadata from the store.
func (se *Series) Drop(ctx context.Context) error {
	if se == nil || se.store == nil {
		return ErrNilParam
	}
	return se.store.DeleteSeries(ctx, se.name)
}
