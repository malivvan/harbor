package ts

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func openBlockStoreForCoverage(t *testing.T, root []byte, seed byte, block time.Duration) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ts-block.db")
	store, _ := openStoreAtPath(t, path, root, Config{
		Secret:        testKey(seed),
		BlockDuration: int64(block),
	})
	return store
}

func TestBlockPointAndInsertBlockPoint(t *testing.T) {
	ctx := context.Background()
	store := openBlockStoreForCoverage(t, []byte("ts"), 71, 10*time.Second)
	series := store.Series("s")

	for _, ts := range []int64{0, 5 * int64(time.Second), 15 * int64(time.Second)} {
		require.NoError(t, series.Insert(ctx, Point{Timestamp: ts, Value: float64(ts)}))
	}

	require.NoError(t, store.b.View(func(tx *bolt.Tx) error {
		bucket, err := store.seriesBucket(tx, "s")
		require.NoError(t, err)
		first, ok, err := store.blockPoint(bucket, false)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, int64(0), first.Timestamp)

		last, ok, err := store.blockPoint(bucket, true)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, 15*int64(time.Second), last.Timestamp)
		return nil
	}))

	// Empty series bucket returns no point.
	require.NoError(t, store.b.Update(func(tx *bolt.Tx) error {
		bucket, err := store.createSeriesBucket(tx, "empty")
		require.NoError(t, err)
		_, ok, err := store.blockPoint(bucket, false)
		require.NoError(t, err)
		require.False(t, ok)
		_, ok, err = store.blockPoint(bucket, true)
		require.NoError(t, err)
		require.False(t, ok)
		return nil
	}))

	// Direct single-point block insertion and replacement.
	require.NoError(t, store.b.Update(func(tx *bolt.Tx) error {
		bucket, err := store.createSeriesBucket(tx, "direct")
		require.NoError(t, err)

		added, err := store.insertBlockPoint(ctx, bucket, Point{Timestamp: 1, Value: 1})
		require.NoError(t, err)
		require.Equal(t, 1, added)

		added, err = store.insertBlockPoint(ctx, bucket, Point{Timestamp: 1, Value: 2})
		require.NoError(t, err)
		require.Equal(t, 0, added)
		return nil
	}))

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, store.b.Update(func(tx *bolt.Tx) error {
		bucket, err := store.createSeriesBucket(tx, "cancel")
		require.NoError(t, err)
		_, err = store.insertBlockPoint(canceled, bucket, Point{Timestamp: 1})
		require.ErrorIs(t, err, context.Canceled)
		return nil
	}))
}

func TestHasDataBranches(t *testing.T) {
	ctx := context.Background()

	// Rootless store: only non-marker metadata entries indicate data.
	store, db := openTestStore(t, nil)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		meta, err := store.metaBucket(tx, true)
		if err != nil {
			return err
		}
		return meta.Put([]byte(markerKey), []byte{1})
	}))
	has, err := store.hasData(db)
	require.NoError(t, err)
	require.False(t, has)

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		meta, err := store.metaBucket(tx, true)
		if err != nil {
			return err
		}
		return meta.Put([]byte("series"), []byte("meta"))
	}))
	has, err = store.hasData(db)
	require.NoError(t, err)
	require.True(t, has)

	// Rooted store: an empty root and a root with only the metadata bucket
	// hold no series.
	rooted, rootDB := openTestStore(t, []byte("ts"))
	require.NoError(t, rootDB.Update(func(tx *bolt.Tx) error {
		_, err := rooted.createRootBucket(tx)
		return err
	}))
	has, err = rooted.hasData(rootDB)
	require.NoError(t, err)
	require.False(t, has)

	require.NoError(t, rootDB.Update(func(tx *bolt.Tx) error {
		meta, err := rooted.metaBucket(tx, true)
		if err != nil {
			return err
		}
		if err := meta.Put([]byte(markerKey), []byte{1}); err != nil {
			return err
		}
		root := rooted.rootBucket(tx)
		_, err = root.CreateBucket([]byte("series"))
		return err
	}))
	has, err = rooted.hasData(rootDB)
	require.NoError(t, err)
	require.True(t, has)

	_ = ctx
}

func TestRetentionMetadataAndDuplicates(t *testing.T) {
	ctx := context.Background()
	store, _ := openTestStore(t, []byte("ts"))
	series := store.Series("s")

	for i := int64(1); i <= 3; i++ {
		require.NoError(t, series.Insert(ctx, Point{Timestamp: int64(time.Second) * i, Value: float64(i)}))
	}

	require.ErrorIs(t, series.SetRetention(ctx, Retention{MaxAge: -1}), ErrInvalidRetention)
	require.NoError(t, series.SetRetention(ctx, Retention{MaxPoints: 2}))
	ret, err := series.Retention(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, ret.MaxPoints)

	// The next insert prunes the oldest point.
	require.NoError(t, series.Insert(ctx, Point{Timestamp: 4 * int64(time.Second), Value: 4}))
	count, err := series.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	// Duplicate timestamps replace the value without growing the series.
	require.NoError(t, series.Insert(ctx, Point{Timestamp: 4 * int64(time.Second), Value: 40}))
	count, err = series.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, count)
	point, err := series.Get(ctx, 4*int64(time.Second))
	require.NoError(t, err)
	require.Equal(t, 40.0, point.Value)

	// MaxAge retention is enforced on demand.
	require.NoError(t, series.SetRetention(ctx, Retention{MaxAge: 2 * time.Second}))
	removed, err := series.EnforceRetention(ctx, 10*int64(time.Second))
	require.NoError(t, err)
	require.Equal(t, 2, removed)

	// Metadata survives SetLabels, and an empty series is a no-op.
	require.NoError(t, series.SetLabels(ctx, map[string]string{"host": "a"}))
	labels, err := series.Labels(ctx)
	require.NoError(t, err)
	require.Equal(t, "a", labels["host"])
	removed, err = store.Series("missing").EnforceRetention(ctx, 0)
	require.NoError(t, err)
	require.Zero(t, removed)
}

func TestLogicalNameEncrypted(t *testing.T) {
	ctx := context.Background()
	store, _ := openStoreAtPath(t, filepath.Join(t.TempDir(), "names.db"), []byte("ts"), Config{
		Secret:             testKey(72),
		EncryptSeriesNames: true,
	})
	require.NoError(t, store.Series("plain").Insert(ctx, Point{Timestamp: 1, Value: 1}))

	physical, err := store.bucketKey("plain")
	require.NoError(t, err)
	name, err := store.logicalName(physical)
	require.NoError(t, err)
	require.Equal(t, "plain", name)

	_, err = store.logicalName([]byte("not-a-sealed-name"))
	require.Error(t, err)

	// A plaintext store maps names one-to-one.
	plain, _ := openTestStore(t, []byte("ts"))
	key, err := plain.bucketKey("name")
	require.NoError(t, err)
	require.Equal(t, []byte("name"), key)
	name, err = plain.logicalName(key)
	require.NoError(t, err)
	require.Equal(t, "name", name)
	name, err = (*Store)(nil).logicalName([]byte("x"))
	require.NoError(t, err)
	require.Equal(t, "x", name)
}

func TestCreateSeriesBucketErrors(t *testing.T) {
	store, db := openTestStore(t, []byte("ts"))
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		_, err := store.createSeriesBucket(tx, "")
		require.Error(t, err)
		return nil
	}))

	// A rooted store creates the root on demand.
	rooted, rootDB := openTestStore(t, []byte("root"))
	require.NoError(t, rootDB.Update(func(tx *bolt.Tx) error {
		b, err := rooted.createSeriesBucket(tx, "s")
		require.NoError(t, err)
		require.NotNil(t, b)
		return nil
	}))
}
