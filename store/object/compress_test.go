package object_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/tempest"
	"github.com/malivvan/tempest/internal/bucket"
	"github.com/malivvan/tempest/store/object"
)

func compressContent() string {
	return strings.Repeat("object content that repeats for compression ", 100)
}

func objectBytes(t *testing.T, db *bolt.DB) int {
	t.Helper()
	var total int
	var walk func(*bolt.Bucket) error
	walk = func(b *bolt.Bucket) error {
		return b.ForEach(func(k, v []byte) error {
			if v != nil {
				total += len(v)
				return nil
			}
			return walk(b.Bucket(k))
		})
	}
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(bucket.OBJECT))
		if root == nil {
			return nil
		}
		return walk(root)
	}))
	return total
}

func TestObjectCompressRoundTrip(t *testing.T) {
	ctx := context.Background()
	key, _ := rotateKeys()
	store, db := openDirect(t, object.Config{Secret: key, EncryptValues: true, Compress: true})

	_, err := store.Put(ctx, "tenant", "docs/big.txt", strings.NewReader(compressContent()), int64(len(compressContent())), "text/plain")
	require.NoError(t, err)

	data, err := store.ReadAll(ctx, "tenant", "docs/big.txt")
	require.NoError(t, err)
	require.Equal(t, compressContent(), string(data))

	info, ok, err := store.Stat(ctx, "tenant", "docs/big.txt")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(len(compressContent())), info.Size)

	infos, err := store.List(ctx, "tenant", "")
	require.NoError(t, err)
	require.Len(t, infos, 1)

	// Compare against an uncompressed store with the same object.
	plain, pdb := openDirect(t, object.Config{Secret: key, EncryptValues: true})
	_, err = plain.Put(ctx, "tenant", "docs/big.txt", strings.NewReader(compressContent()), int64(len(compressContent())), "text/plain")
	require.NoError(t, err)

	compressedSize := objectBytes(t, db)
	plainSize := objectBytes(t, pdb)
	require.Positive(t, compressedSize)
	require.Less(t, compressedSize, plainSize)
}

func TestObjectCompressPlaintextAndRotation(t *testing.T) {
	ctx := context.Background()
	oldKey, newKey := rotateKeys()

	// Plaintext, compressed.
	store, _ := openDirect(t, object.Config{Compress: true})
	_, err := store.Put(ctx, "tenant", "k", strings.NewReader(compressContent()), int64(len(compressContent())), "")
	require.NoError(t, err)
	data, err := store.ReadAll(ctx, "tenant", "k")
	require.NoError(t, err)
	require.Equal(t, compressContent(), string(data))

	// Encrypted, compressed, rotated.
	dbPathStore, db := openDirect(t, object.Config{Secret: oldKey, EncryptValues: true, Compress: true})
	_, err = dbPathStore.Put(ctx, "tenant", "k", strings.NewReader(compressContent()), int64(len(compressContent())), "")
	require.NoError(t, err)
	require.NoError(t, dbPathStore.RotateAll(ctx, newKey))

	again, err := object.Open(ctx, db, object.Config{
		Root:          [][]byte{[]byte(bucket.OBJECT)},
		Secret:        newKey,
		EncryptValues: true,
	})
	require.NoError(t, err)
	data, err = again.ReadAll(ctx, "tenant", "k")
	require.NoError(t, err)
	require.Equal(t, compressContent(), string(data))
}

func TestObjectCompressMigrateFromFile(t *testing.T) {
	ctx := context.Background()
	st, err := tempest.Open(ctx, filepath.Join(t.TempDir(), "tempest.db"), 0o600, &tempest.Options{
		Password:   "object-password",
		KDFTime:    1,
		KDFMemory:  8 * tempest.MiB,
		KDFThreads: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	fsys, err := st.Fstree().Open("blobs")
	require.NoError(t, err)
	require.NoError(t, fsys.MkdirAll("/bucket", 0o755))
	f, err := fsys.Create("/bucket/note.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, st.Objects().MigrateFromFile(ctx, fsys, "bucket"))
	data, err := st.Objects().ReadAll(ctx, "bucket", "note.txt")
	require.NoError(t, err)
	require.Equal(t, "hello", string(data))
}
