package fstree

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/tempest/internal/bucket"
)

func compressContent() string {
	return strings.Repeat("the quick brown fox jumps over the lazy dog. ", 400)
}

// fsContentBytes returns the total stored size of the content and chunk values
// of one filesystem.
func fsContentBytes(t *testing.T, db *bolt.DB, root []byte, name string) int {
	t.Helper()
	var total int
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, fsPath(root, name))
		if os.IsNotExist(err) {
			return nil
		}
		require.NoError(t, err)
		b := newFsBucket(bb)
		for _, target := range []*bolt.Bucket{b.data, b.chunks} {
			if target == nil {
				continue
			}
			if err := target.ForEach(func(_, v []byte) error {
				if v != nil {
					total += len(v)
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}))
	return total
}

func TestFSCompressRoundTrip(t *testing.T) {
	ctx := context.Background()
	content := compressContent()

	for _, chunked := range []bool{false, true} {
		cfg := Config{Secret: testEncKey(1), EncryptContent: true, Compress: true}
		if chunked {
			cfg.ChunkSize = 1024
		}
		root := []byte(bucket.FSTREE)
		store, db := openEncStore(t, root, cfg)
		fsys, err := store.Open("data")
		require.NoError(t, err)
		require.NoError(t, fsys.Mkdir("/dir", 0o755))
		writeTestFile(t, fsys, "/dir/big.txt", content)

		got, err := fsys.ReadFile("/dir/big.txt")
		require.NoError(t, err)
		require.Equal(t, content, string(got))

		stored := fsContentBytes(t, db, root, "data")
		require.Less(t, stored, len(content), "chunked=%v: stored content should shrink", chunked)
		require.NoError(t, ctx.Err())
	}
}

func TestFSCompressTransparentOpen(t *testing.T) {
	ctx := context.Background()
	root := []byte(bucket.FSTREE)
	dbPath := filepath.Join(t.TempDir(), "fs.db")

	wcompressed, db, err := openEncStoreAt(t, dbPath, root, Config{Secret: testEncKey(2), EncryptContent: true, Compress: true})
	require.NoError(t, err)
	fsys, err := wcompressed.Open("data")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/note.txt", compressContent())
	require.NoError(t, db.Close())

	// Reopen with compression disabled: the envelope is still decoded.
	plain, db2, err := openEncStoreAt(t, dbPath, root, Config{Secret: testEncKey(2), EncryptContent: true})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	fsys2, err := plain.Open("data")
	require.NoError(t, err)
	got, err := fsys2.ReadFile("/note.txt")
	require.NoError(t, err)
	require.Equal(t, compressContent(), string(got))
	require.NoError(t, ctx.Err())
}

func TestFSCompressPlaintextStore(t *testing.T) {
	root := []byte(bucket.FSTREE)
	store, db := openEncStore(t, root, Config{Compress: true})
	fsys, err := store.Open("data")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/note.txt", compressContent())

	got, err := fsys.ReadFile("/note.txt")
	require.NoError(t, err)
	require.Equal(t, compressContent(), string(got))
	require.Less(t, fsContentBytes(t, db, root, "data"), len(compressContent()))
}

func TestFSCompressPartialWriteChunked(t *testing.T) {
	root := []byte(bucket.FSTREE)
	store, _ := openEncStore(t, root, Config{
		Secret:        testEncKey(3),
		ChunkSize:     512,
		Compress:      true,
		CompressLevel: 3,
	})
	fsys, err := store.Open("data")
	require.NoError(t, err)

	want := []byte(strings.Repeat("0123456789abcdef", 200))
	writeTestFile(t, fsys, "/file.txt", string(want))

	f, err := fsys.OpenFile("/file.txt", os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("COMPRESSED"), 300)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	copy(want[300:], "COMPRESSED")

	got, err := fsys.ReadFile("/file.txt")
	require.NoError(t, err)
	require.Equal(t, string(want), string(got))

	require.NoError(t, fsys.Truncate("/file.txt", 200))
	got, err = fsys.ReadFile("/file.txt")
	require.NoError(t, err)
	require.Equal(t, string(want[:200]), string(got))
}

func TestFSCompressSnapshotAndReadOnly(t *testing.T) {
	root := []byte(bucket.FSTREE)
	store, _ := openEncStore(t, root, Config{Secret: testEncKey(4), ChunkSize: 1024, Compress: true})
	fsys, err := store.Open("data")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/note.txt", compressContent())

	snap, err := fsys.CreateSnapshot("first")
	require.NoError(t, err)
	defer func() { _ = snap.Release() }()
	got, err := snap.ReadFile("/note.txt")
	require.NoError(t, err)
	require.Equal(t, compressContent(), string(got))

	ro, err := store.OpenReadOnly("data")
	require.NoError(t, err)
	got, err = ro.ReadFile("/note.txt")
	require.NoError(t, err)
	require.Equal(t, compressContent(), string(got))
}

func TestFSCompressRotateAll(t *testing.T) {
	ctx := context.Background()
	oldKey, newKey := testEncKey(5), testEncKey(6)
	root := []byte(bucket.FSTREE)
	dbPath := filepath.Join(t.TempDir(), "fs.db")

	store, db, err := openEncStoreAt(t, dbPath, root, Config{Secret: oldKey, EncryptContent: true, Compress: true})
	require.NoError(t, err)
	fsys, err := store.Open("data")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/note.txt", compressContent())
	require.NoError(t, store.RotateAll(ctx, newKey))
	require.NoError(t, db.Close())

	again, db2, err := openEncStoreAt(t, dbPath, root, Config{Secret: newKey, EncryptContent: true})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	fsys2, err := again.Open("data")
	require.NoError(t, err)
	got, err := fsys2.ReadFile("/note.txt")
	require.NoError(t, err)
	require.Equal(t, compressContent(), string(got))
}
