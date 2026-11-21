package ts

import (
	"context"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestRetentionRoundTripAndClear(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	cpu := store.Series("cpu")

	retention, err := cpu.Retention(ctx)
	require.NoError(t, err)
	require.Equal(t, Retention{}, retention)

	policy := Retention{MaxAge: time.Hour, MaxPoints: 42}
	require.NoError(t, cpu.SetRetention(ctx, policy))
	retention, err = cpu.Retention(ctx)
	require.NoError(t, err)
	require.Equal(t, policy, retention)

	require.NoError(t, cpu.SetRetention(ctx, Retention{}))
	retention, err = cpu.Retention(ctx)
	require.NoError(t, err)
	require.Equal(t, Retention{}, retention)
}

func TestRetentionValidation(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	cpu := store.Series("cpu")

	require.ErrorIs(t, cpu.SetRetention(ctx, Retention{MaxAge: -time.Second}), ErrInvalidRetention)
	require.ErrorIs(t, cpu.SetRetention(ctx, Retention{MaxPoints: -1}), ErrInvalidRetention)
}

func TestLabelsAndRetentionDoNotClobber(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	cpu := store.Series("cpu")

	require.NoError(t, cpu.SetLabels(ctx, map[string]string{"host": "a"}))
	require.NoError(t, cpu.SetRetention(ctx, Retention{MaxPoints: 10}))

	labels, err := cpu.Labels(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"host": "a"}, labels)

	retention, err := cpu.Retention(ctx)
	require.NoError(t, err)
	require.Equal(t, Retention{MaxPoints: 10}, retention)

	require.NoError(t, cpu.SetLabels(ctx, map[string]string{"region": "eu"}))
	retention, err = cpu.Retention(ctx)
	require.NoError(t, err)
	require.Equal(t, Retention{MaxPoints: 10}, retention)

	labels, err = cpu.Labels(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"region": "eu"}, labels)
}

func TestAgeRetentionEnforce(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	cpu := store.Series("cpu")

	require.NoError(t, cpu.InsertMany(ctx, []Point{
		{Timestamp: 0, Value: 0},
		{Timestamp: 100, Value: 1},
		{Timestamp: 200, Value: 2},
		{Timestamp: 300, Value: 3},
	}))
	require.NoError(t, cpu.SetRetention(ctx, Retention{MaxAge: 150 * time.Nanosecond}))

	removed, err := cpu.EnforceRetention(ctx, 300)
	require.NoError(t, err)
	require.Equal(t, 2, removed)

	points, err := cpu.Range(ctx, 0, 400)
	require.NoError(t, err)
	require.Equal(t, []Point{{200, 2}, {300, 3}}, points)
}

func TestCountRetentionEnforce(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	cpu := store.Series("cpu")

	require.NoError(t, cpu.InsertMany(ctx, []Point{
		{Timestamp: 0, Value: 0},
		{Timestamp: 1, Value: 1},
		{Timestamp: 2, Value: 2},
		{Timestamp: 3, Value: 3},
		{Timestamp: 4, Value: 4},
	}))
	require.NoError(t, cpu.SetRetention(ctx, Retention{MaxPoints: 3}))

	removed, err := cpu.EnforceRetention(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, 2, removed)

	points, err := cpu.Range(ctx, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []Point{{2, 2}, {3, 3}, {4, 4}}, points)
}

func TestCombinedRetentionEnforce(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	cpu := store.Series("cpu")

	require.NoError(t, cpu.InsertMany(ctx, []Point{
		{Timestamp: 0, Value: 0},
		{Timestamp: 100, Value: 1},
		{Timestamp: 200, Value: 2},
		{Timestamp: 300, Value: 3},
		{Timestamp: 400, Value: 4},
	}))
	require.NoError(t, cpu.SetRetention(ctx, Retention{
		MaxAge:    350 * time.Nanosecond,
		MaxPoints: 2,
	}))

	removed, err := cpu.EnforceRetention(ctx, 400)
	require.NoError(t, err)
	require.Equal(t, 3, removed, "the larger of the two removal prefixes wins")

	points, err := cpu.Range(ctx, 0, 500)
	require.NoError(t, err)
	require.Equal(t, []Point{{300, 3}, {400, 4}}, points)
}

func TestRetentionMissingSeries(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()

	removed, err := store.Series("missing").EnforceRetention(ctx, 1000)
	require.NoError(t, err)
	require.Equal(t, 0, removed)
}

func TestAutomaticRetentionOnWrite(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	store.SetNow(func() int64 { return 1000 })
	require.Equal(t, int64(1000), store.nowNanos())

	cpu := store.Series("cpu")
	require.NoError(t, cpu.SetRetention(ctx, Retention{MaxAge: 100 * time.Nanosecond}))

	require.NoError(t, cpu.Insert(ctx, Point{Timestamp: 800, Value: 0}))
	require.NoError(t, cpu.Insert(ctx, Point{Timestamp: 950, Value: 1}))
	require.NoError(t, cpu.Insert(ctx, Point{Timestamp: 1000, Value: 2}))

	points, err := cpu.Range(ctx, 0, 2000)
	require.NoError(t, err)
	require.Equal(t, []Point{{950, 1}, {1000, 2}}, points)

	store.SetNow(nil)
	require.WithinDuration(t, time.Now(), time.Unix(0, store.nowNanos()), time.Second)

	limited := store.Series("limited")
	require.NoError(t, limited.SetRetention(ctx, Retention{MaxPoints: 2}))
	require.NoError(t, limited.InsertMany(ctx, []Point{
		{Timestamp: 1, Value: 1},
		{Timestamp: 2, Value: 2},
		{Timestamp: 3, Value: 3},
	}))
	count, err := limited.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, count)
}

func TestStoreWideEnforceRetention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retention.db")
	ctx := context.Background()

	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	store, err := Open(ctx, db, nil, nil)
	require.NoError(t, err)

	age := store.Series("age")
	require.NoError(t, age.InsertMany(ctx, []Point{
		{Timestamp: 0, Value: 0},
		{Timestamp: 500, Value: 1},
		{Timestamp: 1000, Value: 2},
	}))
	require.NoError(t, age.SetRetention(ctx, Retention{MaxAge: 100 * time.Nanosecond}))

	count := store.Series("count")
	require.NoError(t, count.InsertMany(ctx, []Point{
		{Timestamp: 0, Value: 0},
		{Timestamp: 1, Value: 1},
		{Timestamp: 2, Value: 2},
	}))
	require.NoError(t, count.SetRetention(ctx, Retention{MaxPoints: 1}))

	plain := store.Series("plain")
	require.NoError(t, plain.InsertMany(ctx, []Point{
		{Timestamp: 0, Value: 0},
		{Timestamp: 1, Value: 1},
		{Timestamp: 2, Value: 2},
	}))

	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	db, err = bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer db.Close()
	store, err = Open(ctx, db, nil, nil)
	require.NoError(t, err)

	// Do not call Series before enforcement: metadata iteration must cover series
	// that are not cached by this Store instance.
	removed, err := store.EnforceRetention(ctx, 1000)
	require.NoError(t, err)
	require.Equal(t, 4, removed)

	ageCount, err := store.Series("age").Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, ageCount)

	countCount, err := store.Series("count").Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, countCount)

	plainCount, err := store.Series("plain").Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, plainCount)
}

func TestRetentionPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retention-persist.db")
	ctx := context.Background()

	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	store, err := Open(ctx, db, nil, nil)
	require.NoError(t, err)
	cpu := store.Series("cpu")
	require.NoError(t, cpu.SetRetention(ctx, Retention{MaxAge: time.Minute, MaxPoints: 7}))
	require.NoError(t, cpu.SetLabels(ctx, map[string]string{"host": "a"}))
	require.NoError(t, db.Close())

	db, err = bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer db.Close()
	store, err = Open(ctx, db, nil, nil)
	require.NoError(t, err)
	cpu = store.Series("cpu")

	retention, err := cpu.Retention(ctx)
	require.NoError(t, err)
	require.Equal(t, Retention{MaxAge: time.Minute, MaxPoints: 7}, retention)

	labels, err := cpu.Labels(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"host": "a"}, labels)
}

func TestRetentionBackwardCompatibility(t *testing.T) {
	store, db := openTestStore(t, nil)
	ctx := context.Background()

	err := db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists([]byte(metaBucketName))
		if err != nil {
			return err
		}
		return meta.Put([]byte("cpu"), []byte(`{"labels":{"host":"a"}}`))
	})
	require.NoError(t, err)

	cpu := store.Series("cpu")
	retention, err := cpu.Retention(ctx)
	require.NoError(t, err)
	require.Equal(t, Retention{}, retention)

	labels, err := cpu.Labels(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"host": "a"}, labels)
}

func TestRetentionUnderflow(t *testing.T) {
	require.Equal(t, int64(math.MinInt64), retentionCutoff(int64(math.MinInt64)+1, math.MaxInt64))
	require.Equal(t, int64(50), retentionCutoff(100, 50))

	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	cpu := store.Series("cpu")
	require.NoError(t, cpu.InsertMany(ctx, []Point{{Timestamp: 0, Value: 0}, {Timestamp: 100, Value: 1}}))
	require.NoError(t, cpu.SetRetention(ctx, Retention{MaxAge: time.Duration(math.MaxInt64)}))

	removed, err := cpu.EnforceRetention(ctx, int64(math.MinInt64)+1)
	require.NoError(t, err)
	require.Equal(t, 0, removed)
}

func TestRetentionCancellationAndNilReceivers(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cpu := store.Series("cpu")
	require.ErrorIs(t, cpu.SetRetention(ctx, Retention{}), context.Canceled)
	_, err := cpu.Retention(ctx)
	require.ErrorIs(t, err, context.Canceled)
	_, err = cpu.EnforceRetention(ctx, 0)
	require.ErrorIs(t, err, context.Canceled)
	_, err = store.EnforceRetention(ctx, 0)
	require.ErrorIs(t, err, context.Canceled)

	var nilSeries *Series
	require.ErrorIs(t, nilSeries.SetRetention(context.Background(), Retention{}), ErrNilParam)
	_, err = nilSeries.Retention(context.Background())
	require.ErrorIs(t, err, ErrNilParam)
	_, err = nilSeries.EnforceRetention(context.Background(), 0)
	require.ErrorIs(t, err, ErrNilParam)

	var nilStore *Store
	_, err = nilStore.EnforceRetention(context.Background(), 0)
	require.ErrorIs(t, err, ErrNilParam)
	nilStore.SetNow(func() int64 { return 0 })
}

func TestRetentionZeroDoesNotWriteOnInsert(t *testing.T) {
	store, db := openTestStore(t, nil)
	ctx := context.Background()
	cpu := store.Series("cpu")
	require.NoError(t, cpu.InsertMany(ctx, []Point{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}}))

	var keys int
	err := db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte(metaBucketName))
		if meta != nil {
			keys = meta.Stats().KeyN
		}
		return nil
	})
	require.NoError(t, err)
	assert.Zero(t, keys, "inserting without a policy must not create metadata")
}
