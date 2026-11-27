package series

import (
	"context"
	"testing"
	"time"

	hstore "github.com/malivvan/tempest/store"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestPackBlockRoundTrip(t *testing.T) {
	points := []Point{{Timestamp: -5, Value: 1.5}, {Timestamp: 0, Value: -2}, {Timestamp: 10, Value: 3}}
	decoded, err := unpackBlock(packBlock(points))
	require.NoError(t, err)
	require.Equal(t, points, decoded)

	empty, err := unpackBlock(packBlock(nil))
	require.NoError(t, err)
	require.Empty(t, empty)

	_, err = unpackBlock(nil)
	require.Error(t, err)
	_, err = unpackBlock(packBlock(points)[:len(packBlock(points))-1])
	require.Error(t, err)
}

func TestBlockStart(t *testing.T) {
	tests := []struct {
		ts, duration, want int64
	}{
		{0, 10, 0},
		{9, 10, 0},
		{10, 10, 10},
		{-1, 10, -10},
		{-10, 10, -10},
		{-11, 10, -20},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, blockStart(tt.ts, tt.duration), "ts=%d", tt.ts)
	}
}

func openBlockStore(t *testing.T, root []byte, keySeed byte, blockDur int64) *Store {
	t.Helper()
	store, _ := openAEADTestStore(t, root, Config{
		Secret:        testKey(keySeed),
		EncryptValues: true,
		BlockDuration: blockDur,
	})
	return store
}

func blockPoints() []Point {
	return []Point{
		{Timestamp: -5, Value: -5},
		{Timestamp: 0, Value: 0},
		{Timestamp: 5, Value: 5},
		{Timestamp: 9, Value: 9},
		{Timestamp: 10, Value: 10},
		{Timestamp: 15, Value: 15},
		{Timestamp: 19, Value: 19},
		{Timestamp: 20, Value: 20},
		{Timestamp: 25, Value: 25},
	}
}

func TestBlockModeRoundTrip(t *testing.T) {
	ctx := context.Background()
	root := []byte(hstore.SERIES)
	store, db := openAEADTestStore(t, root, Config{
		Secret:        testKey(20),
		EncryptValues: true,
		BlockDuration: 10,
	})
	se := store.Series("cpu")

	points := blockPoints()
	// Out of order, then an overwrite within an existing block.
	require.NoError(t, se.InsertMany(ctx, []Point{points[4], points[0], points[8], points[2], points[6], points[1], points[3], points[5], points[7]}))
	require.NoError(t, se.Insert(ctx, Point{Timestamp: 9, Value: 90}))

	point, err := se.Get(ctx, 9)
	require.NoError(t, err)
	require.Equal(t, Point{Timestamp: 9, Value: 90}, point)

	_, err = se.Get(ctx, 7)
	require.ErrorIs(t, err, ErrNotFound, "missing timestamp inside an existing block")
	_, err = se.Get(ctx, 100)
	require.ErrorIs(t, err, ErrNotFound, "missing block")

	first, err := se.First(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(-5), first.Timestamp)
	last, err := se.Last(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(25), last.Timestamp)

	all, err := se.Range(ctx, -5, 25)
	require.NoError(t, err)
	require.Len(t, all, 9)
	require.Equal(t, Point{Timestamp: 9, Value: 90}, all[3])

	within, err := se.Range(ctx, 6, 12)
	require.NoError(t, err)
	require.Equal(t, []Point{{9, 90}, {10, 10}}, within)

	crossing, err := se.Range(ctx, 9, 20)
	require.NoError(t, err)
	require.Equal(t, []Point{{9, 90}, {10, 10}, {15, 15}, {19, 19}, {20, 20}}, crossing)

	var streamed []Point
	require.NoError(t, se.RangeEach(ctx, 0, 19, func(p Point) error {
		streamed = append(streamed, p)
		return nil
	}))
	require.Equal(t, []Point{{0, 0}, {5, 5}, {9, 90}, {10, 10}, {15, 15}, {19, 19}}, streamed)

	count, err := se.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 9, count)

	samples, err := se.Aggregate(ctx, -10, 29, 10, AggSum)
	require.NoError(t, err)
	require.Equal(t, []Sample{{-10, -5}, {0, 95}, {10, 44}, {20, 45}}, samples)

	// Values on disk are sealed blocks keyed by block starts.
	keys, values := blockBucketContents(t, db, root, "cpu")
	require.Len(t, keys, 4)
	for _, key := range keys {
		require.Len(t, key, recordSize)
	}
	for _, value := range values {
		require.Greater(t, len(value), 16)
	}
	require.Equal(t, []int64{-10, 0, 10, 20}, []int64{
		decodeKey(keys[0]), decodeKey(keys[1]), decodeKey(keys[2]), decodeKey(keys[3]),
	})
}

func blockBucketContents(t *testing.T, db *bolt.DB, root []byte, name string) (keys, values [][]byte) {
	t.Helper()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(root).Bucket([]byte(name))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(key, value []byte) error {
			if value != nil {
				keys = append(keys, append([]byte(nil), key...))
				values = append(values, append([]byte(nil), value...))
			}
			return nil
		})
	}))
	return keys, values
}

func TestBlockModeDeleteAndRetention(t *testing.T) {
	ctx := context.Background()
	root := []byte(hstore.SERIES)
	store := openBlockStore(t, root, 21, 10)
	se := store.Series("cpu")
	require.NoError(t, se.InsertMany(ctx, blockPoints()))

	deleted, err := se.DeleteRange(ctx, 5, 15)
	require.NoError(t, err)
	require.Equal(t, 4, deleted)

	points, err := se.Range(ctx, -5, 25)
	require.NoError(t, err)
	require.Equal(t, []Point{{-5, -5}, {0, 0}, {19, 19}, {20, 20}, {25, 25}}, points)

	count, err := se.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 5, count)

	// Delete an entire block.
	deleted, err = se.DeleteRange(ctx, 20, 29)
	require.NoError(t, err)
	require.Equal(t, 2, deleted)
	_, err = se.Get(ctx, 20)
	require.ErrorIs(t, err, ErrNotFound)

	// Count retention is applied across blocks.
	store2 := openBlockStore(t, root, 22, 10)
	se2 := store2.Series("cpu")
	require.NoError(t, se2.InsertMany(ctx, blockPoints()))
	require.NoError(t, se2.SetRetention(ctx, Retention{MaxPoints: 4}))
	removed, err := se2.EnforceRetention(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, 5, removed)
	points, err = se2.Range(ctx, -5, 25)
	require.NoError(t, err)
	require.Equal(t, []Point{{15, 15}, {19, 19}, {20, 20}, {25, 25}}, points)

	// Age retention.
	store3 := openBlockStore(t, root, 23, 10)
	se3 := store3.Series("cpu")
	require.NoError(t, se3.InsertMany(ctx, blockPoints()))
	require.NoError(t, se3.SetRetention(ctx, Retention{MaxAge: 20 * time.Nanosecond}))
	removed, err = se3.EnforceRetention(ctx, 30)
	require.NoError(t, err)
	require.Equal(t, 4, removed)
	points, err = se3.Range(ctx, -5, 25)
	require.NoError(t, err)
	require.Equal(t, []Point{{10, 10}, {15, 15}, {19, 19}, {20, 20}, {25, 25}}, points)
}

func TestBlockModeMatchesPlaintext(t *testing.T) {
	ctx := context.Background()
	root := []byte(hstore.SERIES)

	plain, _ := openAEADTestStore(t, root, Config{})
	blocked, _ := openAEADTestStore(t, root, Config{
		Secret:        testKey(24),
		EncryptValues: true,
		BlockDuration: 10,
	})

	for _, store := range []*Store{plain, blocked} {
		se := store.Series("cpu")
		require.NoError(t, se.InsertMany(ctx, blockPoints()))
		require.NoError(t, se.Insert(ctx, Point{Timestamp: 9, Value: 90}))
	}

	plainSeries := plain.Series("cpu")
	blockedSeries := blocked.Series("cpu")

	for _, window := range [][2]int64{{-10, 30}, {0, 9}, {9, 20}, {100, 200}} {
		start, end := window[0], window[1]
		want, err := plainSeries.Range(ctx, start, end)
		require.NoError(t, err)
		got, err := blockedSeries.Range(ctx, start, end)
		require.NoError(t, err)
		require.Equal(t, want, got)

		wantCount, err := plainSeries.Count(ctx)
		require.NoError(t, err)
		gotCount, err := blockedSeries.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, wantCount, gotCount)

		wantSamples, err := plainSeries.Aggregate(ctx, start, end, 10, AggSum)
		require.NoError(t, err)
		gotSamples, err := blockedSeries.Aggregate(ctx, start, end, 10, AggSum)
		require.NoError(t, err)
		require.Equal(t, wantSamples, gotSamples)
	}
}

func TestBlockModeTamper(t *testing.T) {
	ctx := context.Background()
	root := []byte(hstore.SERIES)
	store, db := openAEADTestStore(t, root, Config{
		Secret:        testKey(25),
		EncryptValues: true,
		BlockDuration: 10,
	})
	se := store.Series("cpu")
	require.NoError(t, se.InsertMany(ctx, blockPoints()))

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(root).Bucket([]byte("cpu"))
		key := encodeKey(0)
		raw := bucket.Get(key[:])
		require.NotNil(t, raw)
		tampered := append([]byte(nil), raw...)
		tampered[len(tampered)-1] ^= 0xFF
		return bucket.Put(key[:], tampered)
	}))

	_, err := se.Get(ctx, 5)
	require.ErrorIs(t, err, ErrAuthFailed)
	_, err = se.Range(ctx, 0, 9)
	require.ErrorIs(t, err, ErrAuthFailed)
	_, err = se.Count(ctx)
	require.ErrorIs(t, err, ErrAuthFailed)
	_, err = se.Aggregate(ctx, 0, 9, 10, AggSum)
	require.ErrorIs(t, err, ErrAuthFailed)
}

func TestBlockModeConfigMismatch(t *testing.T) {
	ctx := context.Background()
	root := []byte(hstore.SERIES)
	store, db := openAEADTestStore(t, root, Config{
		Secret:        testKey(26),
		EncryptValues: true,
		BlockDuration: 10,
	})
	require.NoError(t, store.Series("cpu").Insert(ctx, Point{Timestamp: 1, Value: 1}))

	_, err := openStore(ctx, db, root, Config{
		Secret:        testKey(26),
		EncryptValues: true,
		BlockDuration: 20,
	})
	require.ErrorIs(t, err, ErrWrongMode)

	_, err = openStore(ctx, db, root, Config{
		Secret:        testKey(26),
		BlockDuration: 10,
	})
	require.ErrorIs(t, err, ErrWrongMode)

	_, err = openStore(ctx, db, root, Config{
		EncryptValues: true,
		BlockDuration: 10,
	})
	require.ErrorIs(t, err, ErrInvalidKey)
}
