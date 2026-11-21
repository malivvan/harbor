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
	Labels map[string]string `json:"labels"`
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

		meta, err := se.store.metaBucket(tx, false)
		if err != nil {
			return err
		}
		if meta == nil {
			return nil
		}
		raw := meta.Get([]byte(se.name))
		if raw == nil {
			return nil
		}

		var stored seriesMeta
		if err := json.Unmarshal(raw, &stored); err != nil {
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

	data, err := json.Marshal(seriesMeta{Labels: labels})
	if err != nil {
		return err
	}

	return se.store.b.Update(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}
		meta, err := se.store.metaBucket(tx, true)
		if err != nil {
			return err
		}
		if err := meta.Put([]byte(se.name), data); err != nil {
			return err
		}
		return checkContext(ctx)
	})
}

// Insert stores one point, replacing any point with the same timestamp.
func (se *Series) Insert(ctx context.Context, p Point) error {
	return se.InsertMany(ctx, []Point{p})
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

		bucket, err := se.store.createSeriesBucket(tx, se.name)
		if err != nil {
			return err
		}

		for _, point := range points {
			if err := checkContext(ctx); err != nil {
				return err
			}
			key := encodeKey(point.Timestamp)
			value := encodeValue(point.Value)
			if err := bucket.Put(key[:], value[:]); err != nil {
				return err
			}
		}
		return checkContext(ctx)
	})
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

		bucket := se.store.seriesBucket(tx, se.name)
		if bucket == nil {
			return ErrNotFound
		}
		key := encodeKey(ts)
		raw := bucket.Get(key[:])
		if raw == nil {
			return ErrNotFound
		}
		if len(raw) != recordSize {
			return fmt.Errorf("ts: corrupt point at %d", ts)
		}
		point = Point{Timestamp: ts, Value: decodeValue(raw)}
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

		bucket := se.store.seriesBucket(tx, se.name)
		if bucket == nil {
			return ErrNotFound
		}
		key, value := bucket.Cursor().First()
		if key == nil {
			return ErrNotFound
		}
		decoded, ok := decodePoint(key, value)
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

		bucket := se.store.seriesBucket(tx, se.name)
		if bucket == nil {
			return ErrNotFound
		}
		key, value := bucket.Cursor().Last()
		if key == nil {
			return ErrNotFound
		}
		decoded, ok := decodePoint(key, value)
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

		bucket := se.store.seriesBucket(tx, se.name)
		if bucket == nil {
			return nil
		}

		startKey := encodeKey(start)
		cursor := bucket.Cursor()
		for key, value := cursor.Seek(startKey[:]); key != nil; key, value = cursor.Next() {
			if err := checkContext(ctx); err != nil {
				return err
			}
			point, ok := decodePoint(key, value)
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

		bucket := se.store.seriesBucket(tx, se.name)
		if bucket == nil {
			return nil
		}

		startKey := encodeKey(start)
		cursor := bucket.Cursor()
		for key, value := cursor.Seek(startKey[:]); key != nil; key, value = cursor.Next() {
			if err := checkContext(ctx); err != nil {
				return err
			}
			point, ok := decodePoint(key, value)
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

	var count int
	err := se.store.b.View(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}
		bucket := se.store.seriesBucket(tx, se.name)
		if bucket != nil {
			count = bucket.Stats().KeyN
		}
		return checkContext(ctx)
	})
	if err != nil {
		return 0, err
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

		bucket := se.store.seriesBucket(tx, se.name)
		if bucket == nil {
			return nil
		}

		var keys [][]byte
		startKey := encodeKey(start)
		cursor := bucket.Cursor()
		for key, value := cursor.Seek(startKey[:]); key != nil; key, value = cursor.Next() {
			if err := checkContext(ctx); err != nil {
				return err
			}
			point, ok := decodePoint(key, value)
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
		return checkContext(ctx)
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
