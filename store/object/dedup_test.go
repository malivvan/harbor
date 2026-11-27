package object_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/tempest/internal/bucket"
	"github.com/malivvan/tempest/store/object"
)

func dedupKey() []byte {
	key, _ := rotateKeys()
	return key
}

func openDedupStore(t *testing.T, cfg object.Config) (*object.Store, *bolt.DB) {
	t.Helper()
	cfg.Dedup = true
	return openDirect(t, cfg)
}

func dedupContent() string {
	return strings.Repeat("deduplicated object content ", 200)
}

// blobCounts returns the number of stored blobs and the sum of their
// reference counts.
func blobCounts(t *testing.T, db *bolt.DB) (blobs int, refs uint64) {
	t.Helper()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(bucket.OBJECT))
		if root == nil {
			return nil
		}
		if b := root.Bucket([]byte("__blobs__")); b != nil {
			_ = b.ForEach(func(_, v []byte) error {
				if v != nil {
					blobs++
				}
				return nil
			})
		}
		if b := root.Bucket([]byte("__blobs_refs__")); b != nil {
			_ = b.ForEach(func(_, v []byte) error {
				if len(v) == 8 {
					var n uint64
					for _, c := range v {
						n = n<<8 | uint64(c)
					}
					refs += n
				}
				return nil
			})
		}
		return nil
	}))
	return blobs, refs
}

func TestObjectDedupAcrossKeys(t *testing.T) {
	ctx := context.Background()
	store, db := openDedupStore(t, object.Config{Secret: dedupKey(), EncryptValues: true})

	for _, target := range [][2]string{{"a", "k"}, {"b", "k"}, {"a", "other"}} {
		_, err := store.Put(ctx, target[0], target[1], strings.NewReader(dedupContent()), int64(len(dedupContent())), "")
		require.NoError(t, err)
	}

	blobs, refs := blobCounts(t, db)
	require.Equal(t, 1, blobs, "identical content must be stored once")
	require.Equal(t, uint64(3), refs)

	for _, target := range [][2]string{{"a", "k"}, {"b", "k"}, {"a", "other"}} {
		data, err := store.ReadAll(ctx, target[0], target[1])
		require.NoError(t, err)
		require.Equal(t, dedupContent(), string(data))
	}
	info, ok, err := store.Stat(ctx, "a", "k")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(len(dedupContent())), info.Size)
}

func TestObjectDedupRefcount(t *testing.T) {
	ctx := context.Background()
	store, db := openDedupStore(t, object.Config{Secret: dedupKey(), EncryptValues: true})

	_, err := store.Put(ctx, "a", "k", strings.NewReader(dedupContent()), int64(len(dedupContent())), "")
	require.NoError(t, err)
	_, err = store.Put(ctx, "b", "k", strings.NewReader(dedupContent()), int64(len(dedupContent())), "")
	require.NoError(t, err)

	require.NoError(t, store.Delete(ctx, "a", "k"))
	blobs, refs := blobCounts(t, db)
	require.Equal(t, 1, blobs, "the blob is still referenced by b")
	require.Equal(t, uint64(1), refs)

	require.NoError(t, store.Delete(ctx, "b", "k"))
	blobs, refs = blobCounts(t, db)
	require.Zero(t, blobs, "the last reference removes the blob")
	require.Zero(t, refs)
}

func TestObjectDedupMove(t *testing.T) {
	ctx := context.Background()
	store, db := openDedupStore(t, object.Config{Secret: dedupKey(), EncryptValues: true})

	_, err := store.Put(ctx, "a", "src", strings.NewReader(dedupContent()), int64(len(dedupContent())), "")
	require.NoError(t, err)
	require.NoError(t, store.Move(ctx, "a", "src", "dst"))

	blobs, refs := blobCounts(t, db)
	require.Equal(t, 1, blobs)
	require.Equal(t, uint64(1), refs)
	data, err := store.ReadAll(ctx, "a", "dst")
	require.NoError(t, err)
	require.Equal(t, dedupContent(), string(data))
}

func TestObjectDedupFailsClosed(t *testing.T) {
	ctx := context.Background()
	key := dedupKey()
	path := filepath.Join(t.TempDir(), "object.db")

	bdb, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 1e9})
	require.NoError(t, err)
	store, err := object.Open(ctx, bdb, object.Config{
		Root:          [][]byte{[]byte(bucket.OBJECT)},
		Secret:        key,
		EncryptValues: true,
		Dedup:         true,
	})
	require.NoError(t, err)
	_, err = store.Put(ctx, "a", "k", strings.NewReader("payload"), 7, "")
	require.NoError(t, err)
	require.NoError(t, bdb.Close())

	for _, cfg := range []object.Config{
		{Root: [][]byte{[]byte(bucket.OBJECT)}, Secret: key, EncryptValues: true},
		{Root: [][]byte{[]byte(bucket.OBJECT)}, Secret: key, Dedup: true},
	} {
		db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 1e9})
		require.NoError(t, err)
		_, err = object.Open(ctx, db, cfg)
		require.ErrorIs(t, err, object.ErrWrongMode)
		require.NoError(t, db.Close())
	}

	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 1e9})
	require.NoError(t, err)
	again, err := object.Open(ctx, db, object.Config{
		Root:          [][]byte{[]byte(bucket.OBJECT)},
		Secret:        key,
		EncryptValues: true,
		Dedup:         true,
	})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	data, err := again.ReadAll(ctx, "a", "k")
	require.NoError(t, err)
	require.Equal(t, "payload", string(data))
}

func TestObjectDedupRotationAndVerification(t *testing.T) {
	ctx := context.Background()
	oldKey, newKey := rotateKeys()
	path := filepath.Join(t.TempDir(), "object.db")

	bdb, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 1e9})
	require.NoError(t, err)
	store, err := object.Open(ctx, bdb, object.Config{
		Root:          [][]byte{[]byte(bucket.OBJECT)},
		Secret:        oldKey,
		EncryptValues: true,
		Compress:      true,
		Dedup:         true,
	})
	require.NoError(t, err)
	_, err = store.Put(ctx, "a", "k", strings.NewReader(dedupContent()), int64(len(dedupContent())), "")
	require.NoError(t, err)
	_, err = store.Put(ctx, "b", "k", strings.NewReader(dedupContent()), int64(len(dedupContent())), "")
	require.NoError(t, err)

	require.NoError(t, store.RotateAll(ctx, newKey))
	require.NoError(t, store.VerifyAll(ctx))
	require.NoError(t, bdb.Close())

	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 1e9})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	again, err := object.Open(ctx, db, object.Config{
		Root:          [][]byte{[]byte(bucket.OBJECT)},
		Secret:        newKey,
		EncryptValues: true,
		Dedup:         true,
	})
	require.NoError(t, err)
	data, err := again.ReadAll(ctx, "b", "k")
	require.NoError(t, err)
	require.Equal(t, dedupContent(), string(data))

	// Deleting after rotation still releases references.
	require.NoError(t, again.Delete(ctx, "a", "k"))
	blobs, refs := blobCounts(t, db)
	require.Equal(t, 1, blobs)
	require.Equal(t, uint64(1), refs)
	require.NoError(t, again.Delete(ctx, "b", "k"))
	blobs, refs = blobCounts(t, db)
	require.Zero(t, blobs)
	require.Zero(t, refs)
}
