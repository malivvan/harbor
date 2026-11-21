package fs

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func rotationConfig(name string, key []byte) Config {
	switch name {
	case "content":
		return Config{Secret: key, EncryptContent: true}
	case "metadata":
		return Config{Secret: key, EncryptMetadata: true}
	case "chunked":
		return Config{Secret: key, ChunkSize: 8}
	default: // both
		return Config{Secret: key, EncryptMetadata: true, EncryptContent: true}
	}
}

func TestReEncryptFS(t *testing.T) {
	for i, name := range []string{"content", "metadata", "both", "chunked"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			root := []byte("ts")
			key1 := testEncKey(byte(40 + i*2))
			key2 := testEncKey(byte(41 + i*2))
			dbPath := filepath.Join(t.TempDir(), "rotate.db")

			store, db, err := openEncStoreAt(t, dbPath, root, rotationConfig(name, key1))
			require.NoError(t, err)
			fsys, err := store.Open("fs")
			require.NoError(t, err)
			require.NoError(t, fsys.MkdirAll("/dir", 0755))
			content := "rotation content across several chunks"
			writeTestFile(t, fsys, "/dir/file.txt", content)
			require.NoError(t, fsys.Symlink("/dir/file.txt", "/link"))

			require.NoError(t, store.ReEncryptFS(ctx, "fs", key2))

			// Existing handles adopt the new key.
			got, err := fsys.ReadFile("/dir/file.txt")
			require.NoError(t, err)
			require.Equal(t, content, string(got))
			target, err := fsys.Readlink("/link")
			require.NoError(t, err)
			require.Equal(t, "/dir/file.txt", target)
			require.NoError(t, store.VerifyAll(ctx))

			require.NoError(t, store.Close())
			require.NoError(t, db.Close())

			// Reopen with the new key.
			store, db, err = openEncStoreAt(t, dbPath, root, rotationConfig(name, key2))
			require.NoError(t, err)
			fsys, err = store.Open("fs")
			require.NoError(t, err)
			got, err = fsys.ReadFile("/dir/file.txt")
			require.NoError(t, err)
			require.Equal(t, content, string(got))
			target, err = fsys.Readlink("/link")
			require.NoError(t, err)
			require.Equal(t, "/dir/file.txt", target)
			require.NoError(t, store.Close())
			require.NoError(t, db.Close())

			// The old key can no longer open the filesystem.
			store, db, err = openEncStoreAt(t, dbPath, root, rotationConfig(name, key1))
			require.NoError(t, err)
			_, err = store.Open("fs")
			require.ErrorIs(t, err, ErrAuthFailed)
			require.NoError(t, store.Close())
			require.NoError(t, db.Close())
		})
	}
}

func TestRotateAll(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")
	key1, key2 := testEncKey(50), testEncKey(51)
	dbPath := filepath.Join(t.TempDir(), "rotate-all.db")

	store, db, err := openEncStoreAt(t, dbPath, root, Config{Secret: key1, EncryptMetadata: true, EncryptContent: true})
	require.NoError(t, err)
	fsA, err := store.Open("a")
	require.NoError(t, err)
	fsB, err := store.Open("b")
	require.NoError(t, err)
	writeTestFile(t, fsA, "/a.txt", "series a")
	writeTestFile(t, fsB, "/b.txt", "series b")

	require.NoError(t, store.RotateAll(ctx, key2))
	require.NoError(t, store.VerifyAll(ctx))

	got, err := fsA.ReadFile("/a.txt")
	require.NoError(t, err)
	require.Equal(t, "series a", string(got))
	got, err = fsB.ReadFile("/b.txt")
	require.NoError(t, err)
	require.Equal(t, "series b", string(got))

	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	store, db, err = openEncStoreAt(t, dbPath, root, Config{Secret: key2, EncryptMetadata: true, EncryptContent: true})
	require.NoError(t, err)
	fsA, err = store.Open("a")
	require.NoError(t, err)
	got, err = fsA.ReadFile("/a.txt")
	require.NoError(t, err)
	require.Equal(t, "series a", string(got))
	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	// Old key fails.
	oldStore, oldDB, err := openEncStoreAt(t, dbPath, root, Config{Secret: key1, EncryptMetadata: true, EncryptContent: true})
	require.NoError(t, err)
	_, err = oldStore.Open("a")
	require.ErrorIs(t, err, ErrAuthFailed)
	require.NoError(t, oldStore.Close())
	require.NoError(t, oldDB.Close())
}

func flipFirstValue(t *testing.T, db *bolt.DB, root []byte, name string, selectBucket func(*fsBucket) *bolt.Bucket) {
	t.Helper()
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, fsPath(root, name))
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		bucket := selectBucket(b)
		require.NotNil(t, bucket)

		var key []byte
		require.NoError(t, bucket.ForEach(func(k, _ []byte) error {
			if key == nil {
				key = append([]byte(nil), k...)
			}
			return nil
		}))
		require.NotNil(t, key)
		raw := append([]byte(nil), bucket.Get(key)...)
		raw[len(raw)-1] ^= 0xFF
		return bucket.Put(key, raw)
	}))
}

func TestVerifyAllTamper(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")

	newStore := func(t *testing.T, cfg Config) (*Store, *bolt.DB, *FileSystem) {
		t.Helper()
		store, db := openEncStore(t, root, cfg)
		fsys, err := store.Open("fs")
		require.NoError(t, err)
		require.NoError(t, fsys.MkdirAll("/dir", 0755))
		writeTestFile(t, fsys, "/dir/file.txt", "verify content")
		require.NoError(t, fsys.Symlink("/dir/file.txt", "/link"))
		require.NoError(t, store.VerifyAll(ctx))
		return store, db, fsys
	}

	t.Run("inode", func(t *testing.T) {
		store, db, _ := newStore(t, Config{Secret: testEncKey(52), EncryptMetadata: true, EncryptContent: true})
		ino := firstUserIno(t, db, root, "fs")
		require.NotZero(t, ino)
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			bb, err := openBucket(tx, fsPath(root, "fs"))
			if err != nil {
				return err
			}
			b := newFsBucket(bb)
			raw := append([]byte(nil), b.inodes.Get(i2b(ino))...)
			raw[len(raw)-1] ^= 0xFF
			return b.inodes.Put(i2b(ino), raw)
		}))
		require.ErrorIs(t, store.VerifyAll(ctx), ErrAuthFailed)
	})

	t.Run("content", func(t *testing.T) {
		store, db, _ := newStore(t, Config{Secret: testEncKey(53), EncryptContent: true})
		flipFirstValue(t, db, root, "fs", func(b *fsBucket) *bolt.Bucket { return b.data })
		require.ErrorIs(t, store.VerifyAll(ctx), ErrAuthFailed)
	})

	t.Run("symlink", func(t *testing.T) {
		store, db, _ := newStore(t, Config{Secret: testEncKey(54), EncryptContent: true})
		flipFirstValue(t, db, root, "fs", func(b *fsBucket) *bolt.Bucket { return b.symlinks })
		require.ErrorIs(t, store.VerifyAll(ctx), ErrAuthFailed)
	})

	t.Run("chunk", func(t *testing.T) {
		store, db, _ := newStore(t, Config{Secret: testEncKey(55), ChunkSize: 8})
		flipFirstValue(t, db, root, "fs", func(b *fsBucket) *bolt.Bucket { return b.chunks })
		require.ErrorIs(t, store.VerifyAll(ctx), ErrAuthFailed)
	})
}

func TestRotationErrors(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")

	plain, _ := openEncStore(t, root, Config{})
	require.ErrorIs(t, plain.ReEncryptFS(ctx, "fs", testEncKey(58)), ErrInvalidConfig)
	require.ErrorIs(t, plain.RotateAll(ctx, testEncKey(58)), ErrInvalidConfig)
	require.NoError(t, plain.VerifyAll(ctx))

	store, _ := openEncStore(t, root, Config{Secret: testEncKey(59), EncryptContent: true})
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/file.txt", "data")

	require.ErrorIs(t, store.ReEncryptFS(ctx, "fs", make([]byte, 16)), ErrInvalidKey)
	require.ErrorIs(t, store.RotateAll(ctx, make([]byte, 16)), ErrInvalidKey)
	require.ErrorIs(t, store.ReEncryptFS(ctx, "", testEncKey(60)), ErrNilParam)
	require.Error(t, store.ReEncryptFS(ctx, "missing", testEncKey(60)))

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, store.ReEncryptFS(canceled, "fs", testEncKey(60)), context.Canceled)
	require.ErrorIs(t, store.RotateAll(canceled, testEncKey(60)), context.Canceled)
	require.ErrorIs(t, store.VerifyAll(canceled), context.Canceled)
}

func TestReEncryptEmptyFS(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")
	key1, key2 := testEncKey(61), testEncKey(62)
	dbPath := filepath.Join(t.TempDir(), "empty.db")

	store, db, err := openEncStoreAt(t, dbPath, root, Config{Secret: key1, EncryptMetadata: true})
	require.NoError(t, err)
	_, err = store.Open("fs")
	require.NoError(t, err)
	require.NoError(t, store.ReEncryptFS(ctx, "fs", key2))
	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	store, db, err = openEncStoreAt(t, dbPath, root, Config{Secret: key2, EncryptMetadata: true})
	require.NoError(t, err)
	defer db.Close()
	defer store.Close()
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/file.txt", "after empty rotation")
	got, err := fsys.ReadFile("/file.txt")
	require.NoError(t, err)
	require.Equal(t, "after empty rotation", string(got))
}

func TestReEncryptFSKeepsOtherFileSystemsReadable(t *testing.T) {
	ctx := context.Background()
	var root []byte
	key1, key2 := testEncKey(70), testEncKey(71)
	dbPath := filepath.Join(t.TempDir(), "rotate-fallback.db")
	cfg := Config{Secret: key1, EncryptContent: true, EncryptMetadata: true}

	store, db, err := openEncStoreAt(t, dbPath, root, cfg)
	require.NoError(t, err)

	fsA, err := store.Open("a")
	require.NoError(t, err)
	require.NoError(t, fsA.MkdirAll("/dir", 0755))
	writeTestFile(t, fsA, "/dir/a.txt", "series a")

	fsB, err := store.Open("b")
	require.NoError(t, err)
	require.NoError(t, fsB.MkdirAll("/dir", 0755))
	writeTestFile(t, fsB, "/dir/b.txt", "series b")

	require.NoError(t, store.ReEncryptFS(ctx, "a", key2))

	// The rotated filesystem reads with the new key...
	got, err := fsA.ReadFile("/dir/a.txt")
	require.NoError(t, err)
	require.Equal(t, "series a", string(got))

	// ...and the one that was not rotated yet keeps working through the
	// retained fallback key.
	got, err = fsB.ReadFile("/dir/b.txt")
	require.NoError(t, err)
	require.Equal(t, "series b", string(got))
	require.NoError(t, store.VerifyAll(ctx))

	// RotateAll rewrites the rest and drops the previous key.
	require.NoError(t, store.RotateAll(ctx, key2))
	require.NoError(t, store.VerifyAll(ctx))
	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	// The new key alone opens both filesystems.
	store, db, err = openEncStoreAt(t, dbPath, root, Config{Secret: key2, EncryptContent: true, EncryptMetadata: true})
	require.NoError(t, err)
	for name, want := range map[string]string{"a": "series a", "b": "series b"} {
		fsys, err := store.Open(name)
		require.NoError(t, err)
		got, err := fsys.ReadFile("/dir/" + name + ".txt")
		require.NoError(t, err)
		require.Equal(t, want, string(got))
	}
	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	// The old key can no longer open either filesystem.
	oldStore, oldDB, err := openEncStoreAt(t, dbPath, root, cfg)
	require.NoError(t, err)
	_, err = oldStore.Open("a")
	require.ErrorIs(t, err, ErrAuthFailed)
	require.NoError(t, oldStore.Close())
	require.NoError(t, oldDB.Close())
}
