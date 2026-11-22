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

	store, err := openStore(context.Background(), db, root, Config{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store, db
}

func TestOpen(t *testing.T) {
	ctx := context.Background()

	store, err := Open(nil, nil, Config{})
	require.ErrorIs(t, err, ErrNilContext)
	require.Nil(t, store)

	store, err = Open(ctx, nil, Config{})
	require.ErrorIs(t, err, ErrNilParam)
	require.Nil(t, store)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	store, err = Open(canceled, nil, Config{})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, store)

	store, db := openTestStore(t, nil)
	require.Same(t, db, store.b)
	require.Nil(t, store.Root())

	root := []byte("tenant")
	store, _ = openTestStore(t, root)
	require.Equal(t, [][]byte{root}, store.Root())
	root[0] = 'X'
	require.Equal(t, [][]byte{[]byte("tenant")}, store.Root(), "Root must return a copy")

	require.NoError(t, store.Close())
	require.NoError(t, db.View(func(tx *bolt.Tx) error { return nil }))

	var nilStore *Store
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
	store, err := Open(ctx, db, Config{})
	require.NoError(t, err)
	cpu := store.Series("cpu")
	require.NoError(t, cpu.InsertMany(ctx, []Point{{Timestamp: 10, Value: 1}, {Timestamp: 20, Value: 2}}))
	require.NoError(t, cpu.SetLabels(ctx, map[string]string{"host": "a"}))
	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	db, err = bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer db.Close()
	store, err = Open(ctx, db, Config{})
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

// These tests fill the gaps left by the behavioural tests in ts_test.go and
// retention_test.go: nil and canceled contexts, closed databases, root-scoped
// buckets, corrupt on-disk records and unusual aggregation windows.

func TestCoverageNilReceivers(t *testing.T) {
	ctx := context.Background()

	var se *Series
	_, err := se.Labels(ctx)
	require.ErrorIs(t, err, ErrNilParam)
	require.ErrorIs(t, se.SetLabels(ctx, nil), ErrNilParam)
	require.ErrorIs(t, se.InsertMany(ctx, []Point{{Timestamp: 1}}), ErrNilParam)
	_, err = se.First(ctx)
	require.ErrorIs(t, err, ErrNilParam)
	_, err = se.Last(ctx)
	require.ErrorIs(t, err, ErrNilParam)
	_, err = se.Range(ctx, 0, 1)
	require.ErrorIs(t, err, ErrNilParam)
	require.ErrorIs(t, se.RangeEach(ctx, 0, 1, func(Point) error { return nil }), ErrNilParam)
	_, err = se.Count(ctx)
	require.ErrorIs(t, err, ErrNilParam)
	_, err = se.DeleteRange(ctx, 0, 1)
	require.ErrorIs(t, err, ErrNilParam)
	_, err = se.Aggregate(ctx, 0, 1, 1, AggSum)
	require.ErrorIs(t, err, ErrNilParam)
	require.ErrorIs(t, se.SetRetention(ctx, Retention{MaxPoints: 1}), ErrNilParam)
	_, err = se.Retention(ctx)
	require.ErrorIs(t, err, ErrNilParam)
	_, err = se.EnforceRetention(ctx, 0)
	require.ErrorIs(t, err, ErrNilParam)

	var store *Store
	require.ErrorIs(t, store.DeleteSeries(ctx, "x"), ErrNilParam)
	_, err = store.EnforceRetention(ctx, 0)
	require.ErrorIs(t, err, ErrNilParam)
	require.NotPanics(t, func() { store.SetNow(nil) })
	require.InDelta(t, time.Now().UnixNano(), store.nowNanos(), float64(2*time.Second))
}

func TestCoverageNilContext(t *testing.T) {
	store, _ := openTestStore(t, nil)
	se := store.Series("cpu")

	_, err := se.Labels(nil)
	require.ErrorIs(t, err, ErrNilContext)
	require.ErrorIs(t, se.SetLabels(nil, nil), ErrNilContext)
	require.ErrorIs(t, se.Insert(nil, Point{Timestamp: 1}), ErrNilContext)
	require.ErrorIs(t, se.InsertMany(nil, []Point{{Timestamp: 1}}), ErrNilContext)
	_, err = se.Get(nil, 1)
	require.ErrorIs(t, err, ErrNilContext)
	_, err = se.First(nil)
	require.ErrorIs(t, err, ErrNilContext)
	_, err = se.Last(nil)
	require.ErrorIs(t, err, ErrNilContext)
	_, err = se.Range(nil, 0, 1)
	require.ErrorIs(t, err, ErrNilContext)
	require.ErrorIs(t, se.RangeEach(nil, 0, 1, func(Point) error { return nil }), ErrNilContext)
	_, err = se.Count(nil)
	require.ErrorIs(t, err, ErrNilContext)
	_, err = se.DeleteRange(nil, 0, 1)
	require.ErrorIs(t, err, ErrNilContext)
	_, err = se.Aggregate(nil, 0, 1, 1, AggSum)
	require.ErrorIs(t, err, ErrNilContext)
	require.ErrorIs(t, se.SetRetention(nil, Retention{MaxPoints: 1}), ErrNilContext)
	_, err = se.Retention(nil)
	require.ErrorIs(t, err, ErrNilContext)
	_, err = se.EnforceRetention(nil, 0)
	require.ErrorIs(t, err, ErrNilContext)
	require.ErrorIs(t, se.Drop(nil), ErrNilContext)

	require.ErrorIs(t, store.DeleteSeries(nil, "cpu"), ErrNilContext)
	_, err = store.EnforceRetention(nil, 0)
	require.ErrorIs(t, err, ErrNilContext)
}

func TestCoverageCanceledContext(t *testing.T) {
	store, _ := openTestStore(t, nil)
	se := store.Series("cpu")
	require.NoError(t, se.Insert(context.Background(), Point{Timestamp: 1, Value: 1}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := se.Labels(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, se.SetLabels(ctx, nil), context.Canceled)
	require.ErrorIs(t, se.Insert(ctx, Point{Timestamp: 2}), context.Canceled)
	require.ErrorIs(t, se.InsertMany(ctx, []Point{{Timestamp: 2}}), context.Canceled)
	_, err = se.Get(ctx, 1)
	require.ErrorIs(t, err, context.Canceled)
	_, err = se.First(ctx)
	require.ErrorIs(t, err, context.Canceled)
	_, err = se.Last(ctx)
	require.ErrorIs(t, err, context.Canceled)
	_, err = se.Range(ctx, 0, 10)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, se.RangeEach(ctx, 0, 10, func(Point) error { return nil }), context.Canceled)
	_, err = se.Count(ctx)
	require.ErrorIs(t, err, context.Canceled)
	_, err = se.DeleteRange(ctx, 0, 10)
	require.ErrorIs(t, err, context.Canceled)
	_, err = se.Aggregate(ctx, 0, 10, 1, AggSum)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, se.SetRetention(ctx, Retention{MaxPoints: 1}), context.Canceled)
	_, err = se.Retention(ctx)
	require.ErrorIs(t, err, context.Canceled)
	_, err = se.EnforceRetention(ctx, 0)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, se.Drop(ctx), context.Canceled)

	require.ErrorIs(t, store.DeleteSeries(ctx, "cpu"), context.Canceled)
	_, err = store.EnforceRetention(ctx, 0)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCoverageClosedDatabase(t *testing.T) {
	store, db := openTestStore(t, nil)
	ctx := context.Background()
	se := store.Series("cpu")
	require.NoError(t, se.Insert(ctx, Point{Timestamp: 1, Value: 1}))
	require.NoError(t, db.Close())

	_, err := se.Labels(ctx)
	require.Error(t, err)
	require.Error(t, se.SetLabels(ctx, map[string]string{"a": "b"}))
	require.Error(t, se.Insert(ctx, Point{Timestamp: 2}))
	require.Error(t, se.InsertMany(ctx, []Point{{Timestamp: 2}}))
	_, err = se.Get(ctx, 1)
	require.Error(t, err)
	_, err = se.First(ctx)
	require.Error(t, err)
	_, err = se.Last(ctx)
	require.Error(t, err)
	_, err = se.Range(ctx, 0, 10)
	require.Error(t, err)
	require.Error(t, se.RangeEach(ctx, 0, 10, func(Point) error { return nil }))
	_, err = se.Count(ctx)
	require.Error(t, err)
	_, err = se.DeleteRange(ctx, 0, 10)
	require.Error(t, err)
	_, err = se.Aggregate(ctx, 0, 10, 1, AggSum)
	require.Error(t, err)
	require.Error(t, se.SetRetention(ctx, Retention{MaxPoints: 1}))
	_, err = se.Retention(ctx)
	require.Error(t, err)
	_, err = se.EnforceRetention(ctx, 0)
	require.Error(t, err)
	require.Error(t, se.Drop(ctx))

	require.Error(t, store.DeleteSeries(ctx, "cpu"))
	_, err = store.EnforceRetention(ctx, 0)
	require.Error(t, err)
}

func TestCoverageEmptyAndMissingSeries(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()

	missing := store.Series("missing")
	_, err := missing.Get(ctx, 1)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = missing.First(ctx)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = missing.Last(ctx)
	require.ErrorIs(t, err, ErrNotFound)
	points, err := missing.Range(ctx, 0, 10)
	require.NoError(t, err)
	require.Empty(t, points)
	require.NoError(t, missing.RangeEach(ctx, 0, 10, func(Point) error {
		t.Fatal("unexpected point in empty series")
		return nil
	}))
	count, err := missing.Count(ctx)
	require.NoError(t, err)
	require.Zero(t, count)
	deleted, err := missing.DeleteRange(ctx, 0, 10)
	require.NoError(t, err)
	require.Zero(t, deleted)
	samples, err := missing.Aggregate(ctx, 0, 10, 1, AggSum)
	require.NoError(t, err)
	require.Empty(t, samples)

	// Removing every point leaves the bucket in place but empty.
	se := store.Series("emptied")
	require.NoError(t, se.InsertMany(ctx, []Point{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}}))
	deleted, err = se.DeleteRange(ctx, 0, 10)
	require.NoError(t, err)
	require.Equal(t, 2, deleted)

	_, err = se.First(ctx)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = se.Last(ctx)
	require.ErrorIs(t, err, ErrNotFound)
	count, err = se.Count(ctx)
	require.NoError(t, err)
	require.Zero(t, count)
	err = se.RangeEach(ctx, 0, 10, func(Point) error {
		t.Fatal("unexpected point in emptied series")
		return nil
	})
	require.NoError(t, err)

	require.ErrorIs(t, se.RangeEach(ctx, 0, 10, nil), ErrNilParam)
}

func TestCoverageCorruptRecords(t *testing.T) {
	store, db := openTestStore(t, nil)
	ctx := context.Background()

	badKey := []byte{0xFF, 0xFF, 0xFF, 0xFF} // sorts after any 8-byte key
	goodValue := encodeValue(1)
	goodKey := encodeKey(1)
	shortValue := []byte{1, 2, 3}

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bad, err := tx.CreateBucketIfNotExists([]byte("corrupt"))
		if err != nil {
			return err
		}
		if err := bad.Put(badKey, goodValue[:]); err != nil {
			return err
		}

		get, err := tx.CreateBucketIfNotExists([]byte("corrupt-get"))
		if err != nil {
			return err
		}
		return get.Put(goodKey[:], shortValue)
	}))

	corrupt := store.Series("corrupt")
	points, err := corrupt.Range(ctx, 0, math.MaxInt64)
	require.NoError(t, err)
	require.Empty(t, points)

	called := 0
	require.NoError(t, corrupt.RangeEach(ctx, 0, math.MaxInt64, func(Point) error {
		called++
		return nil
	}))
	require.Zero(t, called)

	deleted, err := corrupt.DeleteRange(ctx, 0, math.MaxInt64)
	require.NoError(t, err)
	require.Zero(t, deleted)

	samples, err := corrupt.Aggregate(ctx, 0, math.MaxInt64, 1, AggCount)
	require.NoError(t, err)
	require.Empty(t, samples)

	_, err = corrupt.First(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "corrupt")
	_, err = corrupt.Last(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "corrupt")

	_, err = store.Series("corrupt-get").Get(ctx, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "corrupt point")
}

func TestCoverageCorruptMetadata(t *testing.T) {
	store, db := openTestStore(t, nil)
	ctx := context.Background()

	se := store.Series("cpu")
	require.NoError(t, se.Insert(ctx, Point{Timestamp: 1, Value: 1}))
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists([]byte(metaBucketName))
		if err != nil {
			return err
		}
		return meta.Put([]byte("cpu"), []byte("{not json"))
	}))

	_, err := se.Labels(ctx)
	require.Error(t, err)
	require.Error(t, se.SetLabels(ctx, map[string]string{"host": "b"}))
	require.Error(t, se.Insert(ctx, Point{Timestamp: 2}))
	_, err = se.Retention(ctx)
	require.Error(t, err)
	require.Error(t, se.SetRetention(ctx, Retention{MaxPoints: 1}))
	_, err = se.EnforceRetention(ctx, 0)
	require.Error(t, err)

	// A whole-store retention pass also rejects undecodable metadata.
	_, err = store.EnforceRetention(ctx, 0)
	require.Error(t, err)
}

func TestCoverageStoreRetentionWithoutMetadata(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()

	removed, err := store.EnforceRetention(ctx, 0)
	require.NoError(t, err)
	require.Zero(t, removed)

	// Metadata without data: the series bucket is missing and is skipped.
	require.NoError(t, store.Series("meta-only").SetLabels(ctx, map[string]string{"host": "a"}))
	removed, err = store.EnforceRetention(ctx, 0)
	require.NoError(t, err)
	require.Zero(t, removed)
}

func TestCoverageMetaBucketConflict(t *testing.T) {
	store, db := openTestStore(t, []byte("tenant"))
	ctx := context.Background()

	// Occupy the metadata bucket name with a plain value so creating the
	// metadata bucket fails.
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		root, err := tx.CreateBucketIfNotExists([]byte("tenant"))
		if err != nil {
			return err
		}
		return root.Put([]byte(metaBucketName), []byte("blocked"))
	}))

	se := store.Series("cpu")
	require.Error(t, se.SetLabels(ctx, map[string]string{"host": "a"}))
	require.Error(t, se.SetRetention(ctx, Retention{MaxPoints: 1}))
}

func TestCoverageRootScope(t *testing.T) {
	ctx := context.Background()
	root := []byte("tenant")

	// Reads against a root that does not exist yet are empty, not errors.
	empty, _ := openTestStore(t, root)
	emptySeries := empty.Series("cpu")
	points, err := emptySeries.Range(ctx, 0, 10)
	require.NoError(t, err)
	require.Empty(t, points)
	require.ErrorIs(t, empty.DeleteSeries(ctx, "cpu"), ErrNotFound)

	store, db := openTestStore(t, root)
	se := store.Series("cpu")
	require.NoError(t, se.SetLabels(ctx, map[string]string{"host": "a"}))
	require.NoError(t, se.InsertMany(ctx, []Point{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}}))

	point, err := se.Get(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, Point{Timestamp: 1, Value: 1}, point)

	first, err := se.First(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), first.Timestamp)

	last, err := se.Last(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), last.Timestamp)

	count, err := se.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	var walked []Point
	require.NoError(t, se.RangeEach(ctx, 0, 10, func(p Point) error {
		walked = append(walked, p)
		return nil
	}))
	require.Len(t, walked, 2)

	samples, err := se.Aggregate(ctx, 0, 10, 10, AggSum)
	require.NoError(t, err)
	require.Equal(t, []Sample{{0, 3}}, samples)

	deleted, err := se.DeleteRange(ctx, 1, 1)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)

	// Delete the series bucket and its metadata under the root.
	require.NoError(t, store.DeleteSeries(ctx, "cpu"))
	var seriesExists, metaExists bool
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		tenant := tx.Bucket(root)
		require.NotNil(t, tenant)
		seriesExists = tenant.Bucket([]byte("cpu")) != nil
		meta := tenant.Bucket([]byte(metaBucketName))
		metaExists = meta != nil && meta.Get([]byte("cpu")) != nil
		return nil
	}))
	require.False(t, seriesExists)
	require.False(t, metaExists)

	require.ErrorIs(t, store.DeleteSeries(ctx, ""), ErrNilParam)
}

func TestCoverageSetLabelsNilClears(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	se := store.Series("cpu")

	require.NoError(t, se.SetLabels(ctx, map[string]string{"host": "a"}))
	require.NoError(t, se.SetLabels(ctx, nil))

	labels, err := se.Labels(ctx)
	require.NoError(t, err)
	require.Empty(t, labels)
}

func TestCoverageInsertManyEmpty(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	se := store.Series("cpu")

	require.NoError(t, se.InsertMany(ctx, nil))
	require.NoError(t, se.InsertMany(ctx, []Point{}))

	count, err := se.Count(ctx)
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestCoverageDecodePointRejectsInvalidRecords(t *testing.T) {
	store := &Store{} // plaintext store

	_, ok, err := store.decodePoint(nil, nil)
	require.False(t, ok)
	require.NoError(t, err)
	_, ok, err = store.decodePoint(make([]byte, recordSize-1), make([]byte, recordSize))
	require.False(t, ok)
	require.NoError(t, err)
	_, ok, err = store.decodePoint(make([]byte, recordSize), make([]byte, recordSize+1))
	require.False(t, ok)
	require.NoError(t, err)

	key := encodeKey(7)
	value := encodeValue(1.5)
	point, ok, err := store.decodePoint(key[:], value[:])
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, Point{Timestamp: 7, Value: 1.5}, point)
}

func TestCoverageNowNanos(t *testing.T) {
	store, _ := openTestStore(t, nil)

	store.SetNow(func() int64 { return 42 })
	assert.Equal(t, int64(42), store.nowNanos())

	store.SetNow(nil)
	assert.InDelta(t, time.Now().UnixNano(), store.nowNanos(), float64(2*time.Second))

	empty := &Store{}
	assert.InDelta(t, time.Now().UnixNano(), empty.nowNanos(), float64(2*time.Second))
}

func TestCoverageAggregateEdges(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	se := store.Series("cpu")

	// Within one window the minimum can decrease after the first point.
	require.NoError(t, se.InsertMany(ctx, []Point{
		{Timestamp: 0, Value: 5},
		{Timestamp: 1, Value: 2},
		{Timestamp: 2, Value: 9},
		{Timestamp: 20, Value: 4},
	}))

	samples, err := se.Aggregate(ctx, 0, 10, 10, AggMin)
	require.NoError(t, err)
	require.Equal(t, []Sample{{0, 2}}, samples, "points past end must be ignored")

	samples, err = se.Aggregate(ctx, 0, 10, 10, AggMax)
	require.NoError(t, err)
	require.Equal(t, []Sample{{0, 9}}, samples)
}

func TestCoverageRangeEdges(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	se := store.Series("cpu")

	require.ErrorIs(t, se.RangeEach(ctx, 10, 0, func(Point) error { return nil }), ErrInvalidRange)
	_, err := se.DeleteRange(ctx, 10, 0)
	require.ErrorIs(t, err, ErrInvalidRange)

	require.NoError(t, se.InsertMany(ctx, []Point{
		{Timestamp: 1, Value: 1},
		{Timestamp: 5, Value: 2},
		{Timestamp: 20, Value: 3},
	}))

	var visited []int64
	require.NoError(t, se.RangeEach(ctx, 0, 10, func(p Point) error {
		visited = append(visited, p.Timestamp)
		return nil
	}))
	require.Equal(t, []int64{1, 5}, visited, "iteration must stop before points past end")
}

func TestCoverageInsertIntoBucketKey(t *testing.T) {
	store, db := openTestStore(t, nil)
	ctx := context.Background()

	// A nested bucket where a point is expected makes the write fail.
	key := encodeKey(5)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("cpu"))
		if err != nil {
			return err
		}
		_, err = bucket.CreateBucket(key[:])
		return err
	}))

	require.Error(t, store.Series("cpu").Insert(ctx, Point{Timestamp: 5, Value: 1}))
}

// flipContext reports cancellation only after remaining successful Err calls,
// which lets tests reach the cancellation checks that run inside transactions
// and iteration loops.
type flipContext struct {
	context.Context
	remaining int
}

func (c *flipContext) Err() error {
	if c.remaining > 0 {
		c.remaining--
		return nil
	}
	return context.Canceled
}

func flipAfter(n int) context.Context {
	return &flipContext{Context: context.Background(), remaining: n}
}

func TestCoverageCancellationAtTransactionStart(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	se := store.Series("cpu")
	require.NoError(t, se.InsertMany(ctx, []Point{{Timestamp: 1, Value: 1}}))
	require.NoError(t, se.SetLabels(ctx, map[string]string{"host": "a"}))
	require.NoError(t, se.SetRetention(ctx, Retention{MaxPoints: 5}))

	_, err := se.Labels(flipAfter(1))
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, se.SetLabels(flipAfter(1), map[string]string{"host": "b"}), context.Canceled)
	require.ErrorIs(t, se.Insert(flipAfter(1), Point{Timestamp: 2}), context.Canceled)
	require.ErrorIs(t, se.InsertMany(flipAfter(1), []Point{{Timestamp: 2}}), context.Canceled)
	_, err = se.Get(flipAfter(1), 1)
	require.ErrorIs(t, err, context.Canceled)
	_, err = se.First(flipAfter(1))
	require.ErrorIs(t, err, context.Canceled)
	_, err = se.Last(flipAfter(1))
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, se.SetRetention(flipAfter(1), Retention{MaxPoints: 4}), context.Canceled)
	_, err = se.Retention(flipAfter(1))
	require.ErrorIs(t, err, context.Canceled)
	_, err = se.EnforceRetention(flipAfter(1), 0)
	require.ErrorIs(t, err, context.Canceled)
	_, err = se.Range(flipAfter(1), 0, 10)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, se.RangeEach(flipAfter(1), 0, 10, func(Point) error { return nil }), context.Canceled)
	_, err = se.Count(flipAfter(1))
	require.ErrorIs(t, err, context.Canceled)
	_, err = se.DeleteRange(flipAfter(1), 0, 10)
	require.ErrorIs(t, err, context.Canceled)
	_, err = se.Aggregate(flipAfter(1), 0, 10, 1, AggSum)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, store.DeleteSeries(flipAfter(1), "cpu"), context.Canceled)
	_, err = store.EnforceRetention(flipAfter(1), 0)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCoverageCancellationAtLoopCheckpoints(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	se := store.Series("cpu")
	require.NoError(t, se.InsertMany(ctx, []Point{
		{Timestamp: 1, Value: 1},
		{Timestamp: 2, Value: 2},
	}))
	require.NoError(t, se.SetLabels(ctx, map[string]string{"host": "a"}))

	_, err := se.Range(flipAfter(2), 0, 10)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, se.RangeEach(flipAfter(2), 0, 10, func(Point) error { return nil }), context.Canceled)
	_, err = se.DeleteRange(flipAfter(2), 0, 10)
	require.ErrorIs(t, err, context.Canceled)
	_, err = se.Aggregate(flipAfter(2), 0, 10, 1, AggSum)
	require.ErrorIs(t, err, context.Canceled)
	_, err = store.EnforceRetention(flipAfter(2), 0)
	require.ErrorIs(t, err, context.Canceled)

	// InsertMany checks cancellation before every point.
	require.ErrorIs(t, se.InsertMany(flipAfter(2), []Point{{Timestamp: 3, Value: 3}}), context.Canceled)

	// DeleteRange checks cancellation again while deleting the collected keys.
	_, err = se.DeleteRange(flipAfter(4), 0, 10)
	require.ErrorIs(t, err, context.Canceled)
}

func openBenchSeries(b *testing.B) *Series {
	b.Helper()

	db, err := bolt.Open(filepath.Join(b.TempDir(), "ts-bench.db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = db.Close()
	})

	store, err := Open(context.Background(), db, Config{})
	if err != nil {
		b.Fatal(err)
	}
	return store.Series("bench")
}

func BenchmarkInsert(b *testing.B) {
	series := openBenchSeries(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := series.Insert(ctx, Point{Timestamp: int64(i), Value: float64(i)}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsertMany(b *testing.B) {
	series := openBenchSeries(b)
	ctx := context.Background()

	batch := make([]Point, 100)
	for i := range batch {
		batch[i] = Point{Timestamp: int64(i), Value: float64(i)}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := series.InsertMany(ctx, batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRange(b *testing.B) {
	series := openBenchSeries(b)
	ctx := context.Background()

	points := make([]Point, 10_000)
	for i := range points {
		points[i] = Point{Timestamp: int64(i), Value: float64(i)}
	}
	if err := series.InsertMany(ctx, points); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := series.Range(ctx, 0, int64(len(points)-1)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAggregate(b *testing.B) {
	series := openBenchSeries(b)
	ctx := context.Background()

	points := make([]Point, 10_000)
	for i := range points {
		points[i] = Point{Timestamp: int64(i), Value: float64(i)}
	}
	if err := series.InsertMany(ctx, points); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := series.Aggregate(ctx, 0, int64(len(points)-1), 100, AggAvg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEnforceRetention(b *testing.B) {
	series := openBenchSeries(b)
	ctx := context.Background()

	points := make([]Point, 10_000)
	for i := range points {
		points[i] = Point{Timestamp: int64(i), Value: float64(i)}
	}
	if err := series.InsertMany(ctx, points); err != nil {
		b.Fatal(err)
	}
	if err := series.SetRetention(ctx, Retention{MaxPoints: 5_000}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := series.EnforceRetention(ctx, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func TestNestedRoot(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nested.db")
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	root := [][]byte{[]byte("tenant"), []byte("app")}
	store, err := Open(ctx, db, Config{Root: root})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	require.NoError(t, store.Series("cpu").Insert(ctx, Point{Timestamp: 1, Value: 1}))

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		tenant := tx.Bucket([]byte("tenant"))
		require.NotNil(t, tenant)
		app := tenant.Bucket([]byte("app"))
		require.NotNil(t, app)
		require.NotNil(t, app.Bucket([]byte("cpu")))
		return nil
	}))
}
