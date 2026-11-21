package kv_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/malivvan/storm/codec"
	"github.com/malivvan/storm/codec/gob"
	stormdb "github.com/malivvan/storm/db"
	"github.com/malivvan/storm/kv"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func openStore(t *testing.T, root []byte, c codec.MarshalUnmarshaler) (*kv.Store, *bolt.DB) {
	t.Helper()

	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "kv.db"), 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, bdb.Close())
	})

	store, err := kv.Open(context.Background(), bdb, root, c)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	return store, bdb
}

func TestOpen(t *testing.T) {
	ctx := context.Background()

	store, err := kv.Open(ctx, nil, nil, nil)
	require.ErrorIs(t, err, stormdb.ErrNilParam)
	require.Nil(t, store)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	store, err = kv.Open(canceled, nil, nil, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, store)

	store, bdb := openStore(t, nil, nil)
	require.NotNil(t, store)
	require.Same(t, bdb, store.NativeDB())
	require.NotNil(t, store.DB())
	require.NotNil(t, store.Codec())
	require.NoError(t, store.Close())

	// Close must not close the caller-owned bbolt database.
	require.NoError(t, bdb.View(func(tx *bolt.Tx) error { return nil }))
}

func TestStoreBucketsAreCachedAndIsolated(t *testing.T) {
	store, _ := openStore(t, nil, nil)
	ctx := context.Background()

	users := store.Bucket("users")
	require.NotNil(t, users)
	require.Same(t, users, store.Bucket("users"))
	require.Equal(t, "users", users.Name())
	require.Same(t, store, users.Store())

	sessions := store.Bucket("sessions")
	require.Same(t, sessions, store.Bucket("sessions"))
	require.NotSame(t, users, sessions)

	require.NoError(t, users.Set(ctx, "alice", "administrator"))
	require.NoError(t, sessions.Set(ctx, "alice", "session-token"))

	var userRole string
	require.NoError(t, users.Get(ctx, "alice", &userRole))
	require.Equal(t, "administrator", userRole)

	var sessionToken string
	require.NoError(t, sessions.Get(ctx, "alice", &sessionToken))
	require.Equal(t, "session-token", sessionToken)

	require.Equal(t, []string{"users", "sessions"}, store.BucketNames())
	buckets := store.Buckets()
	require.Len(t, buckets, 2)
	require.Same(t, users, buckets[0])
	require.Same(t, sessions, buckets[1])
}

func TestRootBucket(t *testing.T) {
	root := []byte("tenant")
	store, bdb := openStore(t, root, nil)
	ctx := context.Background()

	require.Equal(t, root, store.Root())
	returned := store.Root()
	returned[0] = 'X'
	require.Equal(t, root, store.Root(), "Root must return a copy")

	users := store.Bucket("users")
	require.NoError(t, users.Set(ctx, "alice", "administrator"))

	err := bdb.View(func(tx *bolt.Tx) error {
		tenant := tx.Bucket(root)
		require.NotNil(t, tenant)
		bucket := tenant.Bucket([]byte("users"))
		require.NotNil(t, bucket)
		require.NotNil(t, bucket.Get([]byte("alice")))
		return nil
	})
	require.NoError(t, err)
}

func TestBucketRawOperations(t *testing.T) {
	store, _ := openStore(t, nil, nil)
	ctx := context.Background()
	bucket := store.Bucket("assets")

	require.NoError(t, bucket.SetBytes(ctx, "logo", []byte("png-bytes")))

	raw, err := bucket.GetBytes(ctx, "logo")
	require.NoError(t, err)
	require.Equal(t, []byte("png-bytes"), raw)

	exists, err := bucket.Exists(ctx, "logo")
	require.NoError(t, err)
	require.True(t, exists)

	keys, err := bucket.Keys(ctx)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("logo")}, keys)

	count, err := bucket.Len(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	var iterated int
	require.NoError(t, bucket.ForEach(ctx, func(key, value []byte) error {
		require.Equal(t, []byte("logo"), key)
		require.Equal(t, []byte("png-bytes"), value)
		iterated++
		return nil
	}))
	require.Equal(t, 1, iterated)

	require.NoError(t, bucket.Delete(ctx, "logo"))
	exists, err = bucket.Exists(ctx, "logo")
	require.NoError(t, err)
	require.False(t, exists)

	keys, err = bucket.Keys(ctx)
	require.NoError(t, err)
	require.Empty(t, keys)
}

func TestBucketErrorsAndContext(t *testing.T) {
	store, _ := openStore(t, nil, nil)
	ctx := context.Background()
	bucket := store.Bucket("missing")

	var value string
	require.ErrorIs(t, bucket.Get(ctx, "key", &value), stormdb.ErrNotFound)
	_, err := bucket.GetBytes(ctx, "key")
	require.ErrorIs(t, err, stormdb.ErrNotFound)
	_, err = bucket.Keys(ctx)
	require.ErrorIs(t, err, stormdb.ErrNotFound)

	var nilCtx context.Context
	require.ErrorIs(t, bucket.Get(nilCtx, "key", &value), stormdb.ErrNilContext)
	require.ErrorIs(t, bucket.ForEach(nilCtx, func(_, _ []byte) error { return nil }), stormdb.ErrNilContext)
	require.ErrorIs(t, store.DeleteBucket(nilCtx, "missing"), stormdb.ErrNilContext)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, bucket.ForEach(canceled, func(_, _ []byte) error { return nil }), context.Canceled)
}

func TestStoreDeleteBucket(t *testing.T) {
	store, _ := openStore(t, nil, nil)
	ctx := context.Background()

	users := store.Bucket("users")
	require.NoError(t, users.Set(ctx, "alice", "administrator"))

	require.NoError(t, users.Drop(ctx))
	require.Empty(t, store.BucketNames())

	_, err := users.Exists(ctx, "alice")
	require.ErrorIs(t, err, stormdb.ErrNotFound)

	require.ErrorIs(t, store.DeleteBucket(ctx, "users"), stormdb.ErrNotFound)
	require.ErrorIs(t, store.DeleteBucket(ctx, ""), stormdb.ErrNilParam)
}

func TestBucketWithCodec(t *testing.T) {
	store, _ := openStore(t, nil, nil)
	ctx := context.Background()

	bucket := store.Bucket("records").WithCodec(gob.Codec)
	require.NotNil(t, bucket)
	require.Equal(t, gob.Codec.Name(), bucket.Codec().Name())
	require.Same(t, bucket, bucket.WithCodec(nil))

	type record struct {
		Name string
	}

	require.NoError(t, bucket.Set(ctx, "one", record{Name: "gob"}))
	var got record
	require.NoError(t, bucket.Get(ctx, "one", &got))
	require.Equal(t, "gob", got.Name)
}
