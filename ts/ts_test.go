package ts

import (
	"bytes"
	"context"
	"errors"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func openTestStore(t *testing.T, root []byte) (*Store, *bolt.DB) {
	t.Helper()

	db, err := bolt.Open(filepath.Join(t.TempDir(), "ts.db"), 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	store, err := Open(context.Background(), db, root)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store, db
}

func TestOpen(t *testing.T) {
	ctx := context.Background()

	store, err := Open(nil, nil, nil)
	require.ErrorIs(t, err, ErrNilContext)
	require.Nil(t, store)

	store, err = Open(ctx, nil, nil)
	require.ErrorIs(t, err, ErrNilParam)
	require.Nil(t, store)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	store, err = Open(canceled, nil, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, store)

	store, db := openTestStore(t, nil)
	require.Same(t, db, store.NativeDB())
	require.Nil(t, store.Root())

	root := []byte("tenant")
	store, _ = openTestStore(t, root)
	require.Equal(t, root, store.Root())
	root[0] = 'X'
	require.Equal(t, []byte("tenant"), store.Root(), "Root must return a copy")

	require.NoError(t, store.Close())
	require.NoError(t, db.View(func(tx *bolt.Tx) error { return nil }))

	var nilStore *Store
	require.Nil(t, nilStore.NativeDB())
	require.Nil(t, nilStore.Root())
	require.Nil(t, nilStore.Series("x"))
	require.Nil(t, nilStore.SeriesNames())
	require.ErrorIs(t, nilStore.Close(), ErrNilParam)
	require.ErrorIs(t, nilStore.DeleteSeries(ctx, "x"), ErrNilParam)
}

func TestSeriesCachingAndNames(t *testing.T) {
	store, _ := openTestStore(t, nil)

	cpu := store.Series("cpu")
	require.NotNil(t, cpu)
	require.Same(t, cpu, store.Series("cpu"))
	require.Equal(t, "cpu", cpu.Name())
	require.Same(t, store, cpu.Store())

	mem := store.Series("mem")
	require.Same(t, mem, store.Series("mem"))
	require.NotSame(t, cpu, mem)
	require.Equal(t, []string{"cpu", "mem"}, store.SeriesNames())
}

func TestInsertGetFirstLastAndCount(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	cpu := store.Series("cpu")

	require.NoError(t, cpu.InsertMany(ctx, []Point{
		{Timestamp: 300, Value: 3},
		{Timestamp: 100, Value: 1},
		{Timestamp: 200, Value: 2},
	}))
	require.NoError(t, cpu.Insert(ctx, Point{Timestamp: 200, Value: 22}))

	point, err := cpu.Get(ctx, 200)
	require.NoError(t, err)
	require.Equal(t, Point{Timestamp: 200, Value: 22}, point, "last write wins")

	_, err = cpu.Get(ctx, 999)
	require.ErrorIs(t, err, ErrNotFound)

	first, err := cpu.First(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(100), first.Timestamp)

	last, err := cpu.Last(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(300), last.Timestamp)

	count, err := cpu.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, count)
}

func TestRangeAndRangeEach(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	cpu := store.Series("cpu")

	require.NoError(t, cpu.InsertMany(ctx, []Point{
		{Timestamp: 0, Value: 0},
		{Timestamp: 10, Value: 1},
		{Timestamp: 20, Value: 2},
		{Timestamp: 30, Value: 3},
	}))

	points, err := cpu.Range(ctx, 10, 20)
	require.NoError(t, err)
	require.Equal(t, []Point{{10, 1}, {20, 2}}, points, "range is inclusive")

	points, err = cpu.Range(ctx, 11, 19)
	require.NoError(t, err)
	require.Empty(t, points)

	points, err = cpu.Range(ctx, 20, 10)
	require.ErrorIs(t, err, ErrInvalidRange)
	require.Nil(t, points)

	var iterated []Point
	err = cpu.RangeEach(ctx, 0, 30, func(p Point) error {
		iterated = append(iterated, p)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, iterated, 4)

	stop := errors.New("stop")
	count := 0
	err = cpu.RangeEach(ctx, 0, 30, func(Point) error {
		count++
		return stop
	})
	require.ErrorIs(t, err, stop)
	require.Equal(t, 1, count)

	missing := store.Series("missing")
	points, err = missing.Range(ctx, 0, 100)
	require.NoError(t, err)
	require.Empty(t, points)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = cpu.Range(canceled, 0, 30)
	require.ErrorIs(t, err, context.Canceled)
}

func TestLabels(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	cpu := store.Series("cpu")

	labels, err := cpu.Labels(ctx)
	require.NoError(t, err)
	require.Empty(t, labels)

	require.NoError(t, cpu.SetLabels(ctx, map[string]string{"host": "a", "region": "eu"}))
	labels, err = cpu.Labels(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"host": "a", "region": "eu"}, labels)

	require.NoError(t, cpu.SetLabels(ctx, map[string]string{"host": "b"}))
	labels, err = cpu.Labels(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"host": "b"}, labels)
}

func TestDeleteRangeDropAndDeleteSeries(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	cpu := store.Series("cpu")

	require.NoError(t, cpu.InsertMany(ctx, []Point{
		{Timestamp: 0, Value: 0},
		{Timestamp: 10, Value: 1},
		{Timestamp: 20, Value: 2},
		{Timestamp: 30, Value: 3},
		{Timestamp: 40, Value: 4},
	}))
	require.NoError(t, cpu.SetLabels(ctx, map[string]string{"host": "a"}))

	deleted, err := cpu.DeleteRange(ctx, 10, 30)
	require.NoError(t, err)
	require.Equal(t, 3, deleted)

	count, err := cpu.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	_, err = cpu.Get(ctx, 20)
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, cpu.Drop(ctx))
	require.Empty(t, store.SeriesNames())
	require.Empty(t, labelsAfterDrop(t, cpu, ctx))

	require.ErrorIs(t, store.DeleteSeries(ctx, "cpu"), ErrNotFound)
}

func labelsAfterDrop(t *testing.T, series *Series, ctx context.Context) map[string]string {
	t.Helper()
	labels, err := series.Labels(ctx)
	require.NoError(t, err)
	return labels
}

func TestDeleteSeriesRemovesMetadataOnly(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	cpu := store.Series("cpu")

	require.NoError(t, cpu.SetLabels(ctx, map[string]string{"host": "a"}))
	require.NoError(t, store.DeleteSeries(ctx, "cpu"))

	fresh := store.Series("cpu")
	labels, err := fresh.Labels(ctx)
	require.NoError(t, err)
	require.Empty(t, labels)

	_, err = fresh.Count(ctx)
	require.NoError(t, err)
}

func TestPersistenceAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "persist.db")
	ctx := context.Background()

	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	store, err := Open(ctx, db, nil)
	require.NoError(t, err)
	cpu := store.Series("cpu")
	require.NoError(t, cpu.InsertMany(ctx, []Point{{Timestamp: 10, Value: 1}, {Timestamp: 20, Value: 2}}))
	require.NoError(t, cpu.SetLabels(ctx, map[string]string{"host": "a"}))
	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	db, err = bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer db.Close()
	store, err = Open(ctx, db, nil)
	require.NoError(t, err)
	cpu = store.Series("cpu")

	points, err := cpu.Range(ctx, 0, 100)
	require.NoError(t, err)
	require.Equal(t, []Point{{10, 1}, {20, 2}}, points)

	labels, err := cpu.Labels(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"host": "a"}, labels)
}

func TestRootScope(t *testing.T) {
	root := []byte("tenant")
	store, db := openTestStore(t, root)
	ctx := context.Background()
	cpu := store.Series("cpu")
	require.NoError(t, cpu.Insert(ctx, Point{Timestamp: 1, Value: 1}))

	err := db.View(func(tx *bolt.Tx) error {
		tenant := tx.Bucket(root)
		require.NotNil(t, tenant)
		require.NotNil(t, tenant.Bucket([]byte("cpu")))
		require.Nil(t, tx.Bucket([]byte("cpu")))
		return nil
	})
	require.NoError(t, err)
}

func TestAggregate(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	cpu := store.Series("cpu")
	require.NoError(t, cpu.InsertMany(ctx, []Point{
		{Timestamp: 0, Value: 1},
		{Timestamp: 1, Value: 2},
		{Timestamp: 10, Value: 3},
		{Timestamp: 11, Value: 4},
		{Timestamp: 20, Value: 5},
	}))

	tests := []struct {
		fn       AggregateFunc
		expected []Sample
	}{
		{AggMin, []Sample{{0, 1}, {10, 3}, {20, 5}}},
		{AggMax, []Sample{{0, 2}, {10, 4}, {20, 5}}},
		{AggSum, []Sample{{0, 3}, {10, 7}, {20, 5}}},
		{AggAvg, []Sample{{0, 1.5}, {10, 3.5}, {20, 5}}},
		{AggCount, []Sample{{0, 2}, {10, 2}, {20, 1}}},
		{AggFirst, []Sample{{0, 1}, {10, 3}, {20, 5}}},
		{AggLast, []Sample{{0, 2}, {10, 4}, {20, 5}}},
	}
	for _, tc := range tests {
		t.Run(string(tc.fn), func(t *testing.T) {
			samples, err := cpu.Aggregate(ctx, 0, 20, 10, tc.fn)
			require.NoError(t, err)
			require.Equal(t, tc.expected, samples)
		})
	}

	samples, err := cpu.Aggregate(ctx, 0, 20, 100, AggSum)
	require.NoError(t, err)
	require.Equal(t, []Sample{{0, 15}}, samples)

	_, err = cpu.Aggregate(ctx, 0, 20, 0, AggSum)
	require.ErrorIs(t, err, ErrInvalidStep)
	_, err = cpu.Aggregate(ctx, 20, 0, 10, AggSum)
	require.ErrorIs(t, err, ErrInvalidRange)
	_, err = cpu.Aggregate(ctx, 0, 20, 10, AggregateFunc("bogus"))
	require.Error(t, err)
}

func TestAggregateNegativeWindowAlignment(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	cpu := store.Series("cpu")
	require.NoError(t, cpu.InsertMany(ctx, []Point{
		{Timestamp: -1, Value: 1},
		{Timestamp: 0, Value: 2},
	}))

	samples, err := cpu.Aggregate(ctx, -10, 0, 10, AggSum)
	require.NoError(t, err)
	require.Equal(t, []Sample{{-10, 1}, {0, 2}}, samples)
}

func TestEncodeRoundTripAndOrdering(t *testing.T) {
	timestamps := []int64{math.MinInt64, -100, -1, 0, 1, 100, math.MaxInt64}
	var previous []byte
	for _, ts := range timestamps {
		key := encodeKey(ts)
		assert.Equal(t, ts, decodeKey(key[:]))
		if previous != nil {
			assert.Less(t, bytes.Compare(previous, key[:]), 0)
		}
		previous = key[:]
	}

	for _, value := range []float64{0, -1.5, math.Inf(1), math.Inf(-1)} {
		encoded := encodeValue(value)
		assert.Equal(t, value, decodeValue(encoded[:]))
	}
}

func TestNilSeries(t *testing.T) {
	var series *Series
	require.Empty(t, series.Name())
	require.Nil(t, series.Store())
	_, err := series.Get(context.Background(), 1)
	require.ErrorIs(t, err, ErrNilParam)
	require.ErrorIs(t, series.Insert(context.Background(), Point{}), ErrNilParam)
	require.ErrorIs(t, series.Drop(context.Background()), ErrNilParam)
}
