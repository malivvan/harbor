package fstree

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/tempest/internal/bucket"
)

const dedupChunk = 1024

func dedupContent() string {
	// Four distinct full chunks, so every chunk has its own content address.
	var b strings.Builder
	for i := 0; i < 4; i++ {
		b.WriteString(strings.Repeat(string(rune('A'+i)), dedupChunk))
	}
	return b.String()
}

// dedupInspect returns the number of stored chunks, the number of refcount
// records and the sum of the reference counts.
func dedupInspect(t *testing.T, db *bolt.DB, root []byte, name string) (chunks, refs int, total uint64) {
	t.Helper()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, fsPath(root, name))
		if err != nil {
			return nil
		}
		b := newFsBucket(bb)
		if b == nil {
			return nil
		}
		if b.chunks != nil {
			_ = b.chunks.ForEach(func(_, v []byte) error {
				if v != nil {
					chunks++
				}
				return nil
			})
		}
		if b.chunkrefs != nil {
			_ = b.chunkrefs.ForEach(func(_, v []byte) error {
				if len(v) == 8 {
					refs++
					total += binary.BigEndian.Uint64(v)
				}
				return nil
			})
		}
		return nil
	}))
	return chunks, refs, total
}

func openDedupStore(t *testing.T, root []byte, cfg Config) (*Store, *bolt.DB) {
	t.Helper()
	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = dedupChunk
	}
	return openEncStore(t, root, cfg)
}

func TestDedupAcrossFiles(t *testing.T) {
	root := []byte(bucket.FSTREE)
	store, db := openDedupStore(t, root, Config{Secret: testEncKey(11), Dedup: true})
	fsys, err := store.Open("data")
	require.NoError(t, err)

	require.NoError(t, fsys.Mkdir("/d", 0o755))
	writeTestFile(t, fsys, "/d/a.txt", dedupContent())
	writeTestFile(t, fsys, "/d/b.txt", dedupContent())

	for _, name := range []string{"/d/a.txt", "/d/b.txt"} {
		got, err := fsys.ReadFile(name)
		require.NoError(t, err)
		require.Equal(t, dedupContent(), string(got))
	}

	chunks, refs, total := dedupInspect(t, db, root, "data")
	require.Equal(t, 4, chunks, "identical chunks must be stored once")
	require.Equal(t, 4, refs)
	require.Equal(t, uint64(8), total, "each unique chunk is referenced by both files")
}

func TestDedupRefcountOverwrite(t *testing.T) {
	root := []byte(bucket.FSTREE)
	store, db := openDedupStore(t, root, Config{Secret: testEncKey(12), Dedup: true})
	fsys, err := store.Open("data")
	require.NoError(t, err)

	writeTestFile(t, fsys, "/a.txt", dedupContent())
	writeTestFile(t, fsys, "/b.txt", dedupContent())

	// Overwrite the second chunk of b with different content of the same size.
	f, err := fsys.OpenFile("/b.txt", 0|2, 0) // O_RDWR
	require.NoError(t, err)
	_, err = f.WriteAt([]byte(strings.Repeat("Z", dedupChunk)), int64(dedupChunk))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	got, err := fsys.ReadFile("/b.txt")
	require.NoError(t, err)
	require.Contains(t, string(got), strings.Repeat("Z", dedupChunk))
	require.Equal(t, len(dedupContent()), len(got))

	chunks, _, total := dedupInspect(t, db, root, "data")
	require.Equal(t, 5, chunks, "the replaced chunk is kept only for the other file")
	// a: 4 refs, b: 3 shared + 1 new.
	require.Equal(t, uint64(8), total)
}

func TestDedupRefcountTruncateAndRemove(t *testing.T) {
	root := []byte(bucket.FSTREE)
	store, db := openDedupStore(t, root, Config{Secret: testEncKey(13), Dedup: true})
	fsys, err := store.Open("data")
	require.NoError(t, err)

	writeTestFile(t, fsys, "/a.txt", dedupContent())
	writeTestFile(t, fsys, "/b.txt", dedupContent())

	require.NoError(t, fsys.Truncate("/b.txt", 0))
	got, err := fsys.ReadFile("/b.txt")
	require.NoError(t, err)
	require.Empty(t, got)

	chunks, _, total := dedupInspect(t, db, root, "data")
	require.Equal(t, 4, chunks, "truncating one file keeps the shared chunks")
	require.Equal(t, uint64(4), total)

	require.NoError(t, fsys.Remove("/a.txt"))
	chunks, refs, total := dedupInspect(t, db, root, "data")
	require.Zero(t, chunks, "removing the last reference deletes the chunk")
	require.Zero(t, refs)
	require.Zero(t, total)
}

func TestDedupMarkerFailsClosed(t *testing.T) {
	root := []byte(bucket.FSTREE)
	key := testEncKey(14)
	dbPath := filepath.Join(t.TempDir(), "dedup.db")

	store, db, err := openEncStoreAt(t, dbPath, root, Config{Secret: key, ChunkSize: dedupChunk, Dedup: true})
	require.NoError(t, err)
	fsys, err := store.Open("data")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/a.txt", dedupContent())
	require.NoError(t, db.Close())

	// Mismatched dedup settings fail closed when the filesystem is opened.
	for _, cfg := range []Config{
		{Secret: key, EncryptContent: true},
		{Secret: key, ChunkSize: dedupChunk, EncryptContent: true},
	} {
		mismatched, mdb, err := openEncStoreAt(t, dbPath, root, cfg)
		require.NoError(t, err)
		_, err = mismatched.Open("data")
		require.ErrorIs(t, err, ErrWrongMode)
		require.NoError(t, mdb.Close())
	}

	// The matching configuration opens and reads the data.
	again, db2, err := openEncStoreAt(t, dbPath, root, Config{Secret: key, ChunkSize: dedupChunk, Dedup: true})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	fsys2, err := again.Open("data")
	require.NoError(t, err)
	got, err := fsys2.ReadFile("/a.txt")
	require.NoError(t, err)
	require.Equal(t, dedupContent(), string(got))
}

func TestDedupRotateAllAndSnapshot(t *testing.T) {
	ctx := context.Background()
	root := []byte(bucket.FSTREE)
	oldKey, newKey := testEncKey(15), testEncKey(16)
	dbPath := filepath.Join(t.TempDir(), "dedup.db")

	store, db, err := openEncStoreAt(t, dbPath, root, Config{Secret: oldKey, ChunkSize: dedupChunk, Dedup: true})
	require.NoError(t, err)
	fsys, err := store.Open("data")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/a.txt", dedupContent())

	snap, err := fsys.CreateSnapshot("first")
	require.NoError(t, err)
	defer func() { _ = snap.Release() }()
	got, err := snap.ReadFile("/a.txt")
	require.NoError(t, err)
	require.Equal(t, dedupContent(), string(got))

	require.NoError(t, store.RotateAll(ctx, newKey))
	require.NoError(t, store.VerifyAll(ctx))
	require.NoError(t, db.Close())

	again, db2, err := openEncStoreAt(t, dbPath, root, Config{Secret: newKey, ChunkSize: dedupChunk, Dedup: true})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	fsys2, err := again.Open("data")
	require.NoError(t, err)
	got, err = fsys2.ReadFile("/a.txt")
	require.NoError(t, err)
	require.Equal(t, dedupContent(), string(got))

	// Truncate still releases references after rotation.
	require.NoError(t, fsys2.Truncate("/a.txt", 0))
	_, _, total := dedupInspect(t, db2, root, "data")
	require.Zero(t, total)
}

var _ = bytes.Equal
var _ = errors.Is
