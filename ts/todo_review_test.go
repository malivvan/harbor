package ts

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func openTodoStore(t *testing.T, cfg Config) (*Store, *bolt.DB) {
	t.Helper()

	db, err := bolt.Open(filepath.Join(t.TempDir(), "todo.db"), 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	store, err := Open(context.Background(), db, cfg)
	require.NoError(t, err)
	return store, db
}

// TestConcurrentRotationAndAccess guards T1: rotation must not race with reads
// and writes. Run this test with -race.
func TestConcurrentRotationAndAccess(t *testing.T) {
	ctx := context.Background()
	store, _ := openTodoStore(t, Config{Secret: testKey(50), EncryptValues: true, Root: [][]byte{[]byte("ts")}})
	se := store.Series("cpu")
	require.NoError(t, se.Insert(ctx, Point{Timestamp: 1, Value: 1}))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = se.Range(ctx, 0, 100)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = se.Insert(ctx, Point{Timestamp: int64(i % 50), Value: float64(i)})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = store.RotateAll(ctx, testKey(byte(51+i%3)))
		}
	}()

	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestRotateAllRollsBackOnFailure guards T3: a failed rotation must leave the
// whole store readable with the old key.
func TestRotateAllRollsBackOnFailure(t *testing.T) {
	ctx := context.Background()
	root := [][]byte{[]byte("ts")}
	key1, key2 := testKey(55), testKey(56)
	path := filepath.Join(t.TempDir(), "rollback.db")

	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	store, err := Open(ctx, db, Config{Secret: key1, EncryptValues: true, Root: root})
	require.NoError(t, err)

	require.NoError(t, store.Series("a").Insert(ctx, Point{Timestamp: 1, Value: 1}))
	require.NoError(t, store.Series("b").Insert(ctx, Point{Timestamp: 1, Value: 2}))

	// Corrupt the second series so the rotation aborts after rewriting "a".
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("ts")).Bucket([]byte("b"))
		key := encodeKey(1)
		raw := bucket.Get(key[:])
		raw[len(raw)-1] ^= 0xFF
		return bucket.Put(key[:], raw)
	}))

	err = store.RotateAll(ctx, key2)
	require.ErrorIs(t, err, ErrAuthFailed)

	// "a" was rewritten inside the aborted transaction, so it must still hold
	// its old-key ciphertext.
	points, err := store.Series("a").Range(ctx, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []Point{{1, 1}}, points)

	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	// The old key still opens the store; the marker was never rewritten.
	db, err = bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	store, err = Open(ctx, db, Config{Secret: key1, EncryptValues: true, Root: root})
	require.NoError(t, err)
	points, err = store.Series("a").Range(ctx, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []Point{{1, 1}}, points)
	require.NoError(t, store.Close())
	require.NoError(t, db.Close())
}

// TestZeroRetentionDoesNotScan guards T4: a zero policy must not read (or
// decrypt) the bucket.
func TestZeroRetentionDoesNotScan(t *testing.T) {
	ctx := context.Background()
	store, db := openTodoStore(t, Config{Root: [][]byte{[]byte("ts")}})

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("plain"))
		if err != nil {
			return err
		}
		for i := 0; i < 10; i++ {
			key := encodeKey(int64(i))
			if err := bucket.Put(key[:], []byte("x")); err != nil {
				return err
			}
		}
		n, err := store.pruneSeriesBucket(ctx, bucket, Retention{}, 0)
		require.NoError(t, err)
		require.Zero(t, n)
		count := 0
		cursor := bucket.Cursor()
		for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
			count++
		}
		require.Equal(t, 10, count)

		// A garbage block would fail to decrypt; the zero policy must return
		// before openBlock is ever called.
		blocks, err := tx.CreateBucketIfNotExists([]byte("blocks"))
		if err != nil {
			return err
		}
		key := encodeKey(0)
		if err := blocks.Put(key[:], []byte("not a sealed block")); err != nil {
			return err
		}
		n, err = store.pruneSeriesBucket(ctx, blocks, Retention{}, 0)
		require.NoError(t, err)
		require.Zero(t, n)
		return nil
	}))
}

// TestEncryptionRequiresRoot guards T5: at the database root encrypted stores
// cannot tell series buckets from unrelated top-level buckets.
func TestEncryptionRequiresRoot(t *testing.T) {
	db, err := bolt.Open(filepath.Join(t.TempDir(), "root.db"), 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = Open(context.Background(), db, Config{Secret: testKey(60), EncryptValues: true})
	require.ErrorIs(t, err, ErrInvalidConfig)

	// Plaintext DB-root stores remain supported.
	store, err := Open(context.Background(), db, Config{})
	require.NoError(t, err)
	require.Nil(t, store.Root())
}

// TestEmptyRootSegmentsDoNotPanic guards T7.
func TestEmptyRootSegmentsDoNotPanic(t *testing.T) {
	ctx := context.Background()
	db, err := bolt.Open(filepath.Join(t.TempDir(), "empty-root.db"), 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	store, err := Open(ctx, db, Config{Root: [][]byte{{}}})
	require.NoError(t, err)
	require.Nil(t, store.Root(), "empty segments are filtered out")
	require.NoError(t, store.Series("s").Insert(ctx, Point{Timestamp: 1, Value: 1}))

	_, err = Open(ctx, db, Config{Root: [][]byte{{}}, Secret: testKey(61), EncryptValues: true})
	require.ErrorIs(t, err, ErrInvalidConfig)
}

// TestDeleteSeriesNeverEscapesAMissingRoot guards T6.
func TestDeleteSeriesNeverEscapesAMissingRoot(t *testing.T) {
	ctx := context.Background()
	db, err := bolt.Open(filepath.Join(t.TempDir(), "missing-root.db"), 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// An unrelated top-level bucket that happens to share the series name.
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucket([]byte("cpu"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte("keep"), []byte("me"))
	}))

	store, err := Open(ctx, db, Config{Root: [][]byte{[]byte("missing")}})
	require.NoError(t, err)

	err = store.DeleteSeries(ctx, "cpu")
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("cpu"))
		require.NotNil(t, bucket, "unrelated top-level bucket was deleted")
		require.Equal(t, []byte("me"), bucket.Get([]byte("keep")))
		return nil
	}))
}

// TestEncryptValuesOffKeepsValuesPlaintext guards T8.
func TestEncryptValuesOffKeepsValuesPlaintext(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")
	store, db := openTodoStore(t, Config{Secret: testKey(62), EncryptSeriesNames: true, Root: [][]byte{root}})

	const ts = int64(42)
	require.NoError(t, store.Series("names-only").Insert(ctx, Point{Timestamp: ts, Value: 1.5}))

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		rootBucket := tx.Bucket(root)
		require.NotNil(t, rootBucket)
		var series []byte
		require.NoError(t, rootBucket.ForEach(func(name, value []byte) error {
			if value == nil && !bytes.Equal(name, []byte(metaBucketName)) {
				series = append([]byte(nil), name...)
			}
			return nil
		}))
		require.NotNil(t, series, "encrypted series bucket not found")

		key := encodeKey(ts)
		raw := rootBucket.Bucket(series).Get(key[:])
		require.Len(t, raw, recordSize, "point values must stay 8 plaintext bytes when EncryptValues is off")
		require.Equal(t, 1.5, decodeValue(raw))
		return nil
	}))
}

// ensure imports used by helpers stay referenced.
var _ = errors.Is
var _ = fmt.Sprintf
