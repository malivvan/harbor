package ts

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func tryOpenStoreAtPath(t *testing.T, path string, root []byte, cfg Config) (*Store, *bolt.DB, error) {
	t.Helper()
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	if len(root) > 0 {
		cfg.Root = [][]byte{root}
	}
	store, err := Open(context.Background(), db, cfg)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return store, db, nil
}

func openStoreAtPath(t *testing.T, path string, root []byte, cfg Config) (*Store, *bolt.DB) {
	t.Helper()
	store, db, err := tryOpenStoreAtPath(t, path, root, cfg)
	require.NoError(t, err)
	return store, db
}

func TestReEncryptSeries(t *testing.T) {
	for _, blockDur := range []int64{0, 10} {
		t.Run(fmt.Sprintf("block=%d", blockDur), func(t *testing.T) {
			ctx := context.Background()
			root := []byte("ts")
			key1, key2 := testKey(30), testKey(31)
			path := filepath.Join(t.TempDir(), "rotate.db")

			cfg := Config{Secret: key1, EncryptValues: true, BlockDuration: blockDur}
			store, db := openStoreAtPath(t, path, root, cfg)
			se := store.Series("cpu")
			require.NoError(t, se.InsertMany(ctx, []Point{{Timestamp: 1, Value: 1}, {Timestamp: 5, Value: 5}, {Timestamp: 12, Value: 12}}))
			require.NoError(t, se.SetLabels(ctx, map[string]string{"host": "a"}))
			require.NoError(t, se.SetRetention(ctx, Retention{MaxPoints: 10}))
			require.NoError(t, store.Series("mem").Insert(ctx, Point{Timestamp: 1, Value: 42}))

			require.NoError(t, store.ReEncryptSeries(ctx, "cpu", key2))

			// The series that was not rotated yet keeps working through the
			// retained fallback key.
			memPoints, err := store.Series("mem").Range(ctx, 0, 10)
			require.NoError(t, err)
			require.Equal(t, []Point{{1, 42}}, memPoints)

			points, err := se.Range(ctx, 0, 20)
			require.NoError(t, err)
			require.Equal(t, []Point{{1, 1}, {5, 5}, {12, 12}}, points)
			labels, err := se.Labels(ctx)
			require.NoError(t, err)
			require.Equal(t, map[string]string{"host": "a"}, labels)

			// Completing the rotation in the same process makes key2 the only
			// key needed, after which the store is self-consistent again.
			require.NoError(t, store.RotateAll(ctx, key2))
			require.NoError(t, store.Close())
			require.NoError(t, db.Close())

			_, _, err = tryOpenStoreAtPath(t, path, root, cfg)
			require.ErrorIs(t, err, ErrAuthFailed, "the old key must no longer open the store")

			store, db = openStoreAtPath(t, path, root, Config{Secret: key2, EncryptValues: true, BlockDuration: blockDur})
			points, err = store.Series("cpu").Range(ctx, 0, 20)
			require.NoError(t, err)
			require.Len(t, points, 3)
			require.NoError(t, store.Close())
			require.NoError(t, db.Close())
		})
	}
}

func TestRotateAll(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")
	key1, key2 := testKey(32), testKey(33)
	path := filepath.Join(t.TempDir(), "rotate-all.db")

	cfg := Config{Secret: key1, EncryptValues: true}
	store, db := openStoreAtPath(t, path, root, cfg)
	require.NoError(t, store.Series("cpu").InsertMany(ctx, []Point{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}}))
	require.NoError(t, store.Series("mem").Insert(ctx, Point{Timestamp: 3, Value: 3}))
	require.NoError(t, store.Series("cpu").SetLabels(ctx, map[string]string{"host": "a"}))

	require.NoError(t, store.RotateAll(ctx, key2))
	require.NoError(t, store.VerifyAll(ctx))

	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	_, _, err := tryOpenStoreAtPath(t, path, root, cfg)
	require.ErrorIs(t, err, ErrAuthFailed)

	store, db = openStoreAtPath(t, path, root, Config{Secret: key2, EncryptValues: true})
	defer db.Close()
	defer store.Close()

	points, err := store.Series("cpu").Range(ctx, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []Point{{1, 1}, {2, 2}}, points)
	points, err = store.Series("mem").Range(ctx, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []Point{{3, 3}}, points)
	labels, err := store.Series("cpu").Labels(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"host": "a"}, labels)
}

func TestVerifyAll(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")
	store, db := openAEADTestStore(t, root, Config{Secret: testKey(34), EncryptValues: true})
	se := store.Series("cpu")
	require.NoError(t, se.InsertMany(ctx, []Point{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}}))
	require.NoError(t, store.VerifyAll(ctx))

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(root).Bucket([]byte("cpu"))
		key := encodeKey(1)
		raw := bucket.Get(key[:])
		tampered := append([]byte(nil), raw...)
		tampered[len(tampered)-1] ^= 0xFF
		return bucket.Put(key[:], tampered)
	}))
	require.ErrorIs(t, store.VerifyAll(ctx), ErrAuthFailed)
}

func TestReEncryptSeriesMetadataOnly(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")
	key1, key2 := testKey(35), testKey(36)
	path := filepath.Join(t.TempDir(), "meta.db")

	cfg := Config{Secret: key1, EncryptValues: true}
	store, db := openStoreAtPath(t, path, root, cfg)
	require.NoError(t, store.Series("meta").SetLabels(ctx, map[string]string{"owner": "alice"}))
	require.NoError(t, store.ReEncryptSeries(ctx, "meta", key2))
	// A partial rotation must be completed with RotateAll before closing.
	require.NoError(t, store.RotateAll(ctx, key2))
	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	store, db = openStoreAtPath(t, path, root, Config{Secret: key2, EncryptValues: true})
	defer db.Close()
	defer store.Close()

	labels, err := store.Series("meta").Labels(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"owner": "alice"}, labels)
}

func TestRotationWithEncryptedNames(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")
	key1, key2 := testKey(37), testKey(38)
	path := filepath.Join(t.TempDir(), "names-rotate.db")

	cfg := Config{Secret: key1, EncryptValues: true, EncryptSeriesNames: true, BlockDuration: 10}
	store, db := openStoreAtPath(t, path, root, cfg)
	se := store.Series("cpu")
	require.NoError(t, se.InsertMany(ctx, []Point{{Timestamp: 1, Value: 1}, {Timestamp: 12, Value: 12}}))
	require.NoError(t, se.SetLabels(ctx, map[string]string{"host": "a"}))

	before, _ := physicalKeys(t, db, root)
	require.Len(t, before, 1)

	// A single series cannot be rotated when the names are encrypted: the new
	// key could neither find nor decrypt the remaining series.
	err := store.ReEncryptSeries(ctx, "cpu", key2)
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "RotateAll")

	// RotateAll re-derives every name and keeps the data readable.
	require.NoError(t, store.RotateAll(ctx, key2))
	require.NoError(t, store.VerifyAll(ctx))

	points, err := se.Range(ctx, 0, 20)
	require.NoError(t, err)
	require.Equal(t, []Point{{1, 1}, {12, 12}}, points)
	labels, err := se.Labels(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"host": "a"}, labels)

	after, metaKeys := physicalKeys(t, db, root)
	require.Len(t, after, 1)
	require.NotEqual(t, before[0], after[0], "bucket name must be re-keyed")
	require.Len(t, metaKeys, 1)

	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	// The old key can no longer open the store; the new key reads it.
	_, _, err = tryOpenStoreAtPath(t, path, root, cfg)
	require.ErrorIs(t, err, ErrAuthFailed)

	store, db = openStoreAtPath(t, path, root, Config{Secret: key2, EncryptValues: true, EncryptSeriesNames: true, BlockDuration: 10})
	defer db.Close()
	defer store.Close()
	points, err = store.Series("cpu").Range(ctx, 0, 20)
	require.NoError(t, err)
	require.Equal(t, []Point{{1, 1}, {12, 12}}, points)
}

func TestRotationErrors(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")

	plain, _ := openAEADTestStore(t, root, Config{})
	require.ErrorIs(t, plain.ReEncryptSeries(ctx, "cpu", testKey(41)), ErrInvalidConfig)
	require.ErrorIs(t, plain.RotateAll(ctx, testKey(41)), ErrInvalidConfig)
	require.NoError(t, plain.VerifyAll(ctx))

	store, _ := openAEADTestStore(t, root, Config{Secret: testKey(42), EncryptValues: true})
	require.NoError(t, store.Series("cpu").Insert(ctx, Point{Timestamp: 1, Value: 1}))

	require.ErrorIs(t, store.ReEncryptSeries(ctx, "missing", testKey(43)), ErrNotFound)
	require.ErrorIs(t, store.ReEncryptSeries(ctx, "cpu", make([]byte, 16)), ErrInvalidKey)
	require.ErrorIs(t, store.RotateAll(ctx, make([]byte, 16)), ErrInvalidKey)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, store.ReEncryptSeries(canceled, "cpu", testKey(44)), context.Canceled)
	require.ErrorIs(t, store.RotateAll(canceled, testKey(44)), context.Canceled)
	require.ErrorIs(t, store.VerifyAll(canceled), context.Canceled)
}

// TestReEncryptSeriesRejectedWithEncryptedNames proves a single series cannot be
// rotated when the deterministic series names are encrypted: the new key could
// neither find nor decrypt the remaining series, so the call is rejected.
func TestReEncryptSeriesRejectedWithEncryptedNames(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")
	key1, key2 := testKey(50), testKey(51)
	path := filepath.Join(t.TempDir(), "rotate-names.db")

	cfg := Config{Secret: key1, EncryptValues: true, EncryptSeriesNames: true}
	store, db := openStoreAtPath(t, path, root, cfg)
	defer db.Close()
	defer store.Close()

	require.NoError(t, store.Series("cpu").InsertMany(ctx, []Point{{Timestamp: 1, Value: 1}}))

	err := store.ReEncryptSeries(ctx, "cpu", key2)
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "RotateAll")

	// The store is untouched and still readable.
	points, err := store.Series("cpu").Range(ctx, 0, 10)
	require.NoError(t, err)
	require.Len(t, points, 1)
	require.NoError(t, store.VerifyAll(ctx))
}

// TestReEncryptSeriesPartialRotation covers T2: the marker stays under the old
// key until RotateAll, so a fresh process with the old key can still read the
// series that were not rewritten. Completing the rotation is required before
// Close; the new key cannot verify the old marker.
func TestReEncryptSeriesPartialRotation(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")
	key1, key2 := testKey(40), testKey(41)
	path := filepath.Join(t.TempDir(), "partial.db")

	cfg := Config{Secret: key1, EncryptValues: true}
	store, db := openStoreAtPath(t, path, root, cfg)
	require.NoError(t, store.Series("rotated").Insert(ctx, Point{Timestamp: 1, Value: 1}))
	require.NoError(t, store.Series("pending").Insert(ctx, Point{Timestamp: 1, Value: 2}))
	require.NoError(t, store.ReEncryptSeries(ctx, "rotated", key2))
	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	// The old marker lets the old key open the store and read the un-rotated
	// series. The rotated series is new-key ciphertext.
	store, db = openStoreAtPath(t, path, root, cfg)
	points, err := store.Series("pending").Range(ctx, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []Point{{1, 2}}, points)
	_, err = store.Series("rotated").Range(ctx, 0, 10)
	require.ErrorIs(t, err, ErrAuthFailed)
	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	// The new key cannot verify the old marker yet.
	_, _, err = tryOpenStoreAtPath(t, path, root, Config{Secret: key2, EncryptValues: true})
	require.ErrorIs(t, err, ErrAuthFailed)
}
