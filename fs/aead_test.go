package fs

import (
	"context"
	"errors"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func testEncKey(seed byte) []byte {
	key := make([]byte, KeySize)
	for i := range key {
		key[i] = seed + byte(i)
	}
	return key
}

func openEncStore(t *testing.T, root []byte, cfg Config) (*Store, *bolt.DB) {
	t.Helper()
	db, err := bolt.Open(filepath.Join(t.TempDir(), "fs-enc.db"), 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	if len(root) > 0 {
		cfg.Root = [][]byte{root}
	}
	store, err := Open(context.Background(), db, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store, db
}

func openEncStoreAt(t *testing.T, dbPath string, root []byte, cfg Config) (*Store, *bolt.DB, error) {
	t.Helper()
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: time.Second})
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

func fsPath(root []byte, name string) string {
	if len(root) == 0 {
		return name
	}
	return path.Join(string(root), name)
}

// fsState returns a copy of a state-bucket value.
func fsState(t *testing.T, db *bolt.DB, root []byte, name, key string) []byte {
	t.Helper()
	var out []byte
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, fsPath(root, name))
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		out = append([]byte(nil), b.state.Get([]byte(key))...)
		return nil
	}))
	return out
}

func fsValue(t *testing.T, db *bolt.DB, root []byte, name string, bucket func(*fsBucket) *bolt.Bucket, ino uint64) []byte {
	t.Helper()
	var out []byte
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, fsPath(root, name))
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		out = append([]byte(nil), bucket(b).Get(i2b(ino))...)
		return nil
	}))
	return out
}

func firstUserIno(t *testing.T, db *bolt.DB, root []byte, name string) uint64 {
	t.Helper()
	var first uint64
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, fsPath(root, name))
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		first = 0
		return b.inodes.ForEach(func(key, _ []byte) error {
			if len(key) != 8 {
				return nil
			}
			ino := b2i(key)
			if ino > 1 && (first == 0 || ino < first) {
				first = ino
			}
			return nil
		})
	}))
	return first
}

func rawDecodeInode(t *testing.T, db *bolt.DB, root []byte, name string, ino uint64) error {
	t.Helper()
	var decodeErr error
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, fsPath(root, name))
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		node := new(iNode)
		decodeErr = b.decodeRawNode(ino, node)
		return nil
	}))
	return decodeErr
}

func writeTestFile(t *testing.T, fsys *FS, path, content string) {
	t.Helper()
	f, err := fsys.Create(path)
	require.NoError(t, err)
	_, err = f.Write([]byte(content))
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func TestEncryptionOptionalPlaintextFormat(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")
	store, db := openEncStore(t, root, Config{})

	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/file.txt", "plain hello")

	require.Empty(t, fsState(t, db, root, "fs", markerKey), "plaintext filesystems write no marker")

	ino := firstUserIno(t, db, root, "fs")
	require.NotZero(t, ino)
	require.NoError(t, rawDecodeInode(t, db, root, "fs", ino), "plaintext inodes stay gob-encoded")

	data := fsValue(t, db, root, "fs", func(b *fsBucket) *bolt.Bucket { return b.data }, ino)
	require.Equal(t, []byte("plain hello"), data, "plaintext content is stored raw")

	store2, err := openStore(ctx, db, root, Config{})
	require.NoError(t, err)
	fsys2, err := store2.Open("fs")
	require.NoError(t, err)
	got, err := fsys2.ReadFile("/file.txt")
	require.NoError(t, err)
	require.Equal(t, "plain hello", string(got))
}

func TestEncryptMetadataAndContentRoundTrip(t *testing.T) {
	root := []byte("ts")
	store, db := openEncStore(t, root, Config{
		Secret:          testEncKey(1),
		EncryptMetadata: true,
		EncryptContent:  true,
	})

	fsys, err := store.Open("fs")
	require.NoError(t, err)

	require.NoError(t, fsys.MkdirAll("/dir", 0755))
	writeTestFile(t, fsys, "/dir/file.txt", "secret content")

	got, err := fsys.ReadFile("/dir/file.txt")
	require.NoError(t, err)
	require.Equal(t, "secret content", string(got))

	entries, err := fsys.ReadDir("/dir")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "file.txt", entries[0].Name())

	info, err := fsys.Stat("/dir/file.txt")
	require.NoError(t, err)
	require.Equal(t, int64(len("secret content")), info.Size())

	require.NoError(t, fsys.Rename("/dir/file.txt", "/dir/moved.txt"))
	got, err = fsys.ReadFile("/dir/moved.txt")
	require.NoError(t, err)
	require.Equal(t, "secret content", string(got))

	f, err := fsys.OpenFile("/dir/moved.txt", os.O_RDWR, 0)
	require.NoError(t, err)
	require.NoError(t, f.Truncate(6))
	require.NoError(t, f.Close())
	got, err = fsys.ReadFile("/dir/moved.txt")
	require.NoError(t, err)
	require.Equal(t, "secret", string(got))

	require.NoError(t, fsys.Symlink("/dir/moved.txt", "/link"))
	target, err := fsys.Readlink("/link")
	require.NoError(t, err)
	require.Equal(t, "/dir/moved.txt", target)

	snap, err := fsys.CreateSnapshot("snap")
	require.NoError(t, err)
	snapData, err := snap.ReadFile("/dir/moved.txt")
	require.NoError(t, err)
	require.Equal(t, "secret", string(snapData))
	require.NoError(t, snap.Release())

	// Marker and sealed on-disk records.
	markerRaw := fsState(t, db, root, "fs", markerKey)
	require.NotEmpty(t, markerRaw)
	require.Equal(t, markerMagic, string(markerRaw[:4]))

	ino := firstUserIno(t, db, root, "fs")
	require.NotZero(t, ino)
	require.Error(t, rawDecodeInode(t, db, root, "fs", ino), "inode metadata must be sealed")
	require.NotContains(t, string(fsValue(t, db, root, "fs", func(b *fsBucket) *bolt.Bucket { return b.inodes }, ino)), "secret")
	require.NotEqual(t, []byte("secret"), fsValue(t, db, root, "fs", func(b *fsBucket) *bolt.Bucket { return b.data }, ino))
}

func TestEncryptContentOnly(t *testing.T) {
	root := []byte("ts")
	store, db := openEncStore(t, root, Config{Secret: testEncKey(2), EncryptContent: true})
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/file.txt", "content-only")

	ino := firstUserIno(t, db, root, "fs")
	require.NoError(t, rawDecodeInode(t, db, root, "fs", ino), "metadata stays plaintext")
	require.NotEqual(t, []byte("content-only"), fsValue(t, db, root, "fs", func(b *fsBucket) *bolt.Bucket { return b.data }, ino))
}

func TestEncryptMetadataOnly(t *testing.T) {
	root := []byte("ts")
	store, db := openEncStore(t, root, Config{Secret: testEncKey(3), EncryptMetadata: true})
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/file.txt", "metadata-only")

	ino := firstUserIno(t, db, root, "fs")
	require.Error(t, rawDecodeInode(t, db, root, "fs", ino), "inode metadata must be sealed")
	require.Equal(t, []byte("metadata-only"), fsValue(t, db, root, "fs", func(b *fsBucket) *bolt.Bucket { return b.data }, ino))
}

func TestEncryptionWrongKey(t *testing.T) {
	root := []byte("ts")
	dbPath := filepath.Join(t.TempDir(), "wrong-key.db")

	store, db, err := openEncStoreAt(t, dbPath, root, Config{Secret: testEncKey(4), EncryptMetadata: true, EncryptContent: true})
	require.NoError(t, err)
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/file.txt", "secret")
	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	store, db, err = openEncStoreAt(t, dbPath, root, Config{Secret: testEncKey(5), EncryptMetadata: true, EncryptContent: true})
	require.NoError(t, err)
	_, err = store.Open("fs")
	require.ErrorIs(t, err, ErrAuthFailed)
	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	store, db, err = openEncStoreAt(t, dbPath, root, Config{Secret: testEncKey(4), EncryptMetadata: true, EncryptContent: true})
	require.NoError(t, err)
	defer db.Close()
	defer store.Close()
	fsys, err = store.Open("fs")
	require.NoError(t, err)
	got, err := fsys.ReadFile("/file.txt")
	require.NoError(t, err)
	require.Equal(t, "secret", string(got))
}

func TestEncryptionTamper(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")
	cfg := Config{Secret: testEncKey(6), EncryptMetadata: true, EncryptContent: true}
	store, db := openEncStore(t, root, cfg)
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/file.txt", "tamper me")
	ino := firstUserIno(t, db, root, "fs")
	require.NoError(t, store.Close())

	// Tamper with the sealed inode, then open a fresh store (empty cache).
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
	store2, err := openStore(ctx, db, root, cfg)
	require.NoError(t, err)
	fsys2, err := store2.Open("fs")
	require.NoError(t, err)
	_, err = fsys2.ReadFile("/file.txt")
	require.ErrorIs(t, err, ErrAuthFailed)
	require.NoError(t, store2.Close())

	// Tamper with the sealed content.
	store3, err := openStore(ctx, db, root, cfg)
	require.NoError(t, err)
	fsys3, err := store3.Open("fs")
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, fsPath(root, "fs"))
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		raw := append([]byte(nil), b.data.Get(i2b(ino))...)
		raw[len(raw)-1] ^= 0xFF
		return b.data.Put(i2b(ino), raw)
	}))
	_, err = fsys3.ReadFile("/file.txt")
	require.ErrorIs(t, err, ErrAuthFailed)
}

func TestEncryptionNonceFreshness(t *testing.T) {
	root := []byte("ts")
	store, db := openEncStore(t, root, Config{Secret: testEncKey(7), EncryptContent: true})
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/a.txt", "same value")
	writeTestFile(t, fsys, "/b.txt", "same value")

	// Locate both file inodes by their data-bucket keys.
	var inos []uint64
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, fsPath(root, "fs"))
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		return b.data.ForEach(func(key, _ []byte) error {
			inos = append(inos, b2i(key))
			return nil
		})
	}))
	require.Len(t, inos, 2)
	first := fsValue(t, db, root, "fs", func(b *fsBucket) *bolt.Bucket { return b.data }, inos[0])
	second := fsValue(t, db, root, "fs", func(b *fsBucket) *bolt.Bucket { return b.data }, inos[1])
	require.NotEqual(t, first, second, "fresh nonces must make equal plaintext differ")
}

func TestEncryptionModeDetection(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")

	// Plaintext data then encrypted open is rejected.
	plain, db := openEncStore(t, root, Config{})
	fsys, err := plain.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/file.txt", "plain")

	encStore, err := openStore(ctx, db, root, Config{Secret: testEncKey(8), EncryptContent: true})
	require.NoError(t, err)
	_, err = encStore.Open("fs")
	require.ErrorIs(t, err, ErrWrongMode, "plaintext data must not be silently encrypted")

	// Encrypted filesystem opened without a key is rejected.
	root2 := []byte("ts2")
	enc, db2 := openEncStore(t, root2, Config{Secret: testEncKey(9), EncryptContent: true})
	fsys, err = enc.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/file.txt", "secret")
	plainStore, err := openStore(ctx, db2, root2, Config{})
	require.NoError(t, err)
	_, err = plainStore.Open("fs")
	require.ErrorIs(t, err, ErrWrongMode)

	// Different encryption settings are rejected.
	other, err := openStore(ctx, db2, root2, Config{Secret: testEncKey(9), EncryptMetadata: true})
	require.NoError(t, err)
	_, err = other.Open("fs")
	require.ErrorIs(t, err, ErrWrongMode)
}

func TestEncryptionEmptyPlaintextFSMigration(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")

	plain, db := openEncStore(t, root, Config{})
	fsys, err := plain.Open("fs")
	require.NoError(t, err)
	require.NoError(t, fsys.Close())

	enc, err := openStore(ctx, db, root, Config{Secret: testEncKey(10), EncryptMetadata: true, EncryptContent: true})
	require.NoError(t, err)
	encFS, err := enc.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, encFS, "/file.txt", "converted")
	got, err := encFS.ReadFile("/file.txt")
	require.NoError(t, err)
	require.Equal(t, "converted", string(got))
}

func TestEncryptionConfigValidation(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")
	_, db := openEncStore(t, root, Config{})

	tests := []struct {
		name string
		cfg  Config
		want error
	}{
		{"missing key", Config{EncryptContent: true}, ErrInvalidKey},
		{"short key", Config{Secret: make([]byte, 16), EncryptContent: true}, ErrInvalidKey},
		{"negative chunk", Config{Secret: testEncKey(11), ChunkSize: -1}, ErrInvalidConfig},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := openStore(ctx, db, root, tt.cfg)
			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestEncryptionMarkerFlags(t *testing.T) {
	root := []byte("ts")
	store, db := openEncStore(t, root, Config{
		Secret:          testEncKey(14),
		EncryptMetadata: true,
		EncryptContent:  true,
	})
	_, err := store.Open("fs")
	require.NoError(t, err)

	raw := fsState(t, db, root, "fs", markerKey)
	require.NotEmpty(t, raw)
	m, canary, err := decodeMarker(raw)
	require.NoError(t, err)
	require.True(t, m.encContent)
	require.True(t, m.encMetadata)
	require.Zero(t, m.chunkSize)
	require.NotEmpty(t, canary)
}
