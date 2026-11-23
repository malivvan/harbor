package file

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func chunkedStore(t *testing.T, root []byte, keySeed byte, chunkSize int) (*Store, *bolt.DB) {
	t.Helper()
	return openEncStore(t, root, Config{Secret: testEncKey(keySeed), ChunkSize: chunkSize})
}

type chunkEntry struct {
	ino uint64
	idx int64
	raw []byte
}

func chunkEntries(t *testing.T, db *bolt.DB, root []byte, name string) []chunkEntry {
	t.Helper()
	var entries []chunkEntry
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, fsPath(root, name))
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		if b.chunks == nil {
			return nil
		}
		return b.chunks.ForEach(func(key, value []byte) error {
			entries = append(entries, chunkEntry{
				ino: b2i(key[:8]),
				idx: int64(b2i(key[8:])),
				raw: append([]byte(nil), value...),
			})
			return nil
		})
	}))
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ino != entries[j].ino {
			return entries[i].ino < entries[j].ino
		}
		return entries[i].idx < entries[j].idx
	})
	return entries
}

func TestChunkedRoundTrip(t *testing.T) {
	root := []byte("ts")
	store, db := chunkedStore(t, root, 20, 8)
	fsys, err := store.Open("fs")
	require.NoError(t, err)

	content := "0123456789abcdefghij" // 20 bytes -> chunks of 8, 8, 4
	writeTestFile(t, fsys, "/file.txt", content)

	got, err := fsys.ReadFile("/file.txt")
	require.NoError(t, err)
	require.Equal(t, content, string(got))

	info, err := fsys.Stat("/file.txt")
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), info.Size())

	// Sequential reads across chunk boundaries.
	f, err := fsys.Open("/file.txt")
	require.NoError(t, err)
	var streamed []byte
	buf := make([]byte, 3)
	for {
		n, err := f.Read(buf)
		streamed = append(streamed, buf[:n]...)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}
	require.Equal(t, content, string(streamed))

	// Random access.
	f2, err := fsys.Open("/file.txt")
	require.NoError(t, err)
	part := make([]byte, 5)
	n, err := f2.ReadAt(part, 7)
	require.NoError(t, err)
	require.Equal(t, content[7:12], string(part[:n]))

	// Overwrite across a chunk boundary.
	f3, err := fsys.OpenFile("/file.txt", os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f3.WriteAt([]byte("XYZ"), 6)
	require.NoError(t, err)
	require.NoError(t, f3.Close())
	want := content[:6] + "XYZ" + content[9:]
	got, err = fsys.ReadFile("/file.txt")
	require.NoError(t, err)
	require.Equal(t, want, string(got))

	// Append.
	f4, err := fsys.OpenFile("/file.txt", os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f4.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	_, err = f4.Write([]byte("!"))
	require.NoError(t, err)
	require.NoError(t, f4.Close())
	got, err = fsys.ReadFile("/file.txt")
	require.NoError(t, err)
	require.Equal(t, want+"!", string(got))

	// Gap write: bytes between old EOF and the offset read back as zeros.
	f5, err := fsys.OpenFile("/file.txt", os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f5.WriteAt([]byte("Z"), int64(len(want)+10))
	require.NoError(t, err)
	require.NoError(t, f5.Close())
	got, err = fsys.ReadFile("/file.txt")
	require.NoError(t, err)
	require.Len(t, got, len(want)+11)
	require.Equal(t, "Z", string(got[len(got)-1:]))
	for _, b := range got[len(want)+1 : len(got)-1] {
		require.Zero(t, b)
	}

	// Truncate shrinks and extends.
	f6, err := fsys.OpenFile("/file.txt", os.O_RDWR, 0)
	require.NoError(t, err)
	require.NoError(t, f6.Truncate(10))
	require.NoError(t, f6.Close())
	got, err = fsys.ReadFile("/file.txt")
	require.NoError(t, err)
	require.Len(t, got, 10)

	f7, err := fsys.OpenFile("/file.txt", os.O_RDWR, 0)
	require.NoError(t, err)
	require.NoError(t, f7.Truncate(13))
	require.NoError(t, f7.Close())
	got, err = fsys.ReadFile("/file.txt")
	require.NoError(t, err)
	require.Len(t, got, 13)
	for _, b := range got[10:] {
		require.Zero(t, b)
	}

	// On-disk layout: sealed chunks, empty data bucket, chunked marker.
	entries := chunkEntries(t, db, root, "fs")
	require.Len(t, entries, 2) // 10 and 13 bytes with 8-byte chunks
	require.Equal(t, int64(0), entries[0].idx)
	require.Equal(t, int64(1), entries[1].idx)
	for _, entry := range entries {
		require.Greater(t, len(entry.raw), 16)
	}
	require.Empty(t, fsValue(t, db, root, "fs", func(b *fsBucket) *bolt.Bucket { return b.data }, entries[0].ino))

	marker, _, err := decodeMarker(fsState(t, db, root, "fs", markerKey))
	require.NoError(t, err)
	require.Equal(t, 8, marker.chunkSize)
	require.True(t, marker.encContent)
}

func TestChunkedWritesTouchOnlyAffectedChunks(t *testing.T) {
	root := []byte("ts")
	store, db := chunkedStore(t, root, 21, 8)
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/file.txt", "0123456789abcdefghij")

	before := chunkEntries(t, db, root, "fs")
	require.Len(t, before, 3)

	f, err := fsys.OpenFile("/file.txt", os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("X"), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	after := chunkEntries(t, db, root, "fs")
	require.Len(t, after, 3)
	require.NotEqual(t, before[0].raw, after[0].raw, "the written chunk must change")
	require.Equal(t, before[1].raw, after[1].raw, "untouched chunks keep their ciphertext")
	require.Equal(t, before[2].raw, after[2].raw, "untouched chunks keep their ciphertext")

	got, err := fsys.ReadFile("/file.txt")
	require.NoError(t, err)
	require.Equal(t, "X123456789abcdefghij", string(got))
}

func TestChunkedEmptyFile(t *testing.T) {
	root := []byte("ts")
	store, db := chunkedStore(t, root, 22, 8)
	fsys, err := store.Open("fs")
	require.NoError(t, err)

	writeTestFile(t, fsys, "/empty.txt", "")
	got, err := fsys.ReadFile("/empty.txt")
	require.NoError(t, err)
	require.Empty(t, got)
	require.Empty(t, chunkEntries(t, db, root, "fs"))
}

func TestChunkedTamper(t *testing.T) {
	root := []byte("ts")
	cfg := Config{Secret: testEncKey(23), ChunkSize: 8}
	store, db := openEncStore(t, root, cfg)
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/file.txt", "0123456789abcdef")
	require.NoError(t, store.Close())

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, fsPath(root, "fs"))
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		var first []byte
		if err := b.chunks.ForEach(func(key, value []byte) error {
			if first == nil {
				first = append([]byte(nil), key...)
			}
			return nil
		}); err != nil {
			return err
		}
		raw := append([]byte(nil), b.chunks.Get(first)...)
		raw[len(raw)-1] ^= 0xFF
		return b.chunks.Put(first, raw)
	}))

	store2, err := openStore(context.Background(), db, root, cfg)
	require.NoError(t, err)
	fsys2, err := store2.Open("fs")
	require.NoError(t, err)
	_, err = fsys2.ReadFile("/file.txt")
	require.ErrorIs(t, err, ErrAuthFailed)
}

func TestChunkedMatchesPlaintext(t *testing.T) {
	root := []byte("ts")

	plain, _ := openEncStore(t, root, Config{})
	chunked, _ := chunkedStore(t, root, 24, 5)

	for _, store := range []*Store{plain, chunked} {
		fsys, err := store.Open("fs")
		require.NoError(t, err)
		require.NoError(t, fsys.MkdirAll("/dir", 0755))
		writeTestFile(t, fsys, "/dir/a.txt", "hello world, this is chunked content")
		f, err := fsys.OpenFile("/dir/a.txt", os.O_RDWR, 0)
		require.NoError(t, err)
		_, err = f.WriteAt([]byte("HELLO"), 0)
		require.NoError(t, err)
		require.NoError(t, f.Truncate(20))
		require.NoError(t, f.Close())
	}

	plainFS, err := plain.Open("fs")
	require.NoError(t, err)
	chunkedFS, err := chunked.Open("fs")
	require.NoError(t, err)

	want, err := plainFS.ReadFile("/dir/a.txt")
	require.NoError(t, err)
	got, err := chunkedFS.ReadFile("/dir/a.txt")
	require.NoError(t, err)
	require.Equal(t, want, got)

	wantInfo, err := plainFS.Stat("/dir/a.txt")
	require.NoError(t, err)
	gotInfo, err := chunkedFS.Stat("/dir/a.txt")
	require.NoError(t, err)
	require.Equal(t, wantInfo.Size(), gotInfo.Size())

	entries, err := chunkedFS.ReadDir("/dir")
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestChunkedSnapshot(t *testing.T) {
	root := []byte("ts")
	store, _ := chunkedStore(t, root, 25, 4)
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/file.txt", "snapshot content across chunks")

	snap, err := fsys.CreateSnapshot("snap")
	require.NoError(t, err)
	defer snap.Release()

	got, err := snap.ReadFile("/file.txt")
	require.NoError(t, err)
	require.Equal(t, "snapshot content across chunks", string(got))

	f, err := snap.Open("/file.txt")
	require.NoError(t, err)
	buf, err := io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, "snapshot content across chunks", string(buf))
}

func TestChunkedConfigMismatch(t *testing.T) {
	root := []byte("ts")
	dbPath := filepath.Join(t.TempDir(), "chunk-mode.db")

	store, db, err := openEncStoreAt(t, dbPath, root, Config{Secret: testEncKey(26), ChunkSize: 8})
	require.NoError(t, err)
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/file.txt", "data")
	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	// Different chunk size.
	store, db, err = openEncStoreAt(t, dbPath, root, Config{Secret: testEncKey(26), ChunkSize: 16})
	require.NoError(t, err)
	_, err = store.Open("fs")
	require.ErrorIs(t, err, ErrWrongMode)
	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	// Whole-blob content mode instead of chunks.
	store, db, err = openEncStoreAt(t, dbPath, root, Config{Secret: testEncKey(26), EncryptContent: true})
	require.NoError(t, err)
	_, err = store.Open("fs")
	require.ErrorIs(t, err, ErrWrongMode)
	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	// Wrong key.
	store, db, err = openEncStoreAt(t, dbPath, root, Config{Secret: testEncKey(27), ChunkSize: 8})
	require.NoError(t, err)
	_, err = store.Open("fs")
	require.ErrorIs(t, err, ErrAuthFailed)
	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	// Correct settings still read the data.
	store, db, err = openEncStoreAt(t, dbPath, root, Config{Secret: testEncKey(26), ChunkSize: 8})
	require.NoError(t, err)
	defer db.Close()
	defer store.Close()
	fsys, err = store.Open("fs")
	require.NoError(t, err)
	got, err := fsys.ReadFile("/file.txt")
	require.NoError(t, err)
	require.Equal(t, "data", string(got))
}

func TestChunkedOptionalWhenDisabled(t *testing.T) {
	root := []byte("ts")
	store, db := openEncStore(t, root, Config{Secret: testEncKey(28), EncryptContent: true})
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/file.txt", "whole blob")

	// Whole-blob content lives in the data bucket; no chunks bucket is created.
	require.Empty(t, chunkEntries(t, db, root, "fs"))
	ino := firstUserIno(t, db, root, "fs")
	require.NotEmpty(t, fsValue(t, db, root, "fs", func(b *fsBucket) *bolt.Bucket { return b.data }, ino))
}

func TestChunkedZeroLengthWrite(t *testing.T) {
	root := []byte("ts")
	store, db := chunkedStore(t, root, 29, 8)
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/file.txt", "abc")

	f, err := fsys.OpenFile("/file.txt", os.O_RDWR, 0)
	require.NoError(t, err)
	n, err := f.Write(nil)
	require.NoError(t, err)
	require.Zero(t, n)
	require.NoError(t, f.Close())

	got, err := fsys.ReadFile("/file.txt")
	require.NoError(t, err)
	require.Equal(t, "abc", string(got))
	require.Len(t, chunkEntries(t, db, root, "fs"), 1)
}

func TestChunkedRandomAccessRead(t *testing.T) {
	root := []byte("ts")
	store, _ := chunkedStore(t, root, 30, 4)
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/file.txt", "abcdefghijklmnopqrstuvwxyz")

	f, err := fsys.Open("/file.txt")
	require.NoError(t, err)
	for off := int64(0); off < 26; off += 3 {
		buf := make([]byte, 5)
		n, err := f.ReadAt(buf, off)
		end := off + 5
		if end > 26 {
			end = 26
		}
		require.Equal(t, string("abcdefghijklmnopqrstuvwxyz"[off:end]), string(buf[:n]))
		if off+5 > 26 {
			require.Equal(t, io.EOF, err)
		} else {
			require.NoError(t, err)
		}
	}

	// Seeking beyond EOF then back.
	_, err = f.Seek(100, io.SeekStart)
	require.NoError(t, err)
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	buf := make([]byte, 3)
	n, err := f.Read(buf)
	require.NoError(t, err)
	require.Equal(t, "abc", string(buf[:n]))
}

func TestChunkedFreshNonce(t *testing.T) {
	root := []byte("ts")
	store, db := chunkedStore(t, root, 31, 8)
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/a.txt", "same")
	writeTestFile(t, fsys, "/b.txt", "same")

	entries := chunkEntries(t, db, root, "fs")
	require.Len(t, entries, 2)
	require.True(t, entries[0].ino != entries[1].ino)
	require.False(t, bytes.Equal(entries[0].raw, entries[1].raw), "fresh nonces must differ")
}

// TestChunkedWriteDoesNotPadTail verifies that a partial tail chunk is stored
// at its logical length instead of being zero-padded to a full chunk.
func TestChunkedWriteDoesNotPadTail(t *testing.T) {
	root := []byte("ts")
	store, db := chunkedStore(t, root, 33, 1024)
	fsys, err := store.Open("fs")
	require.NoError(t, err)

	writeTestFile(t, fsys, "/small.txt", "0123456789")

	entries := chunkEntries(t, db, root, "fs")
	require.Len(t, entries, 1)
	require.Less(t, len(entries[0].raw), 1024,
		"the sealed tail chunk should be shorter than a full chunk")

	data, err := fsys.ReadFile("/small.txt")
	require.NoError(t, err)
	require.Equal(t, "0123456789", string(data))

	// Growing inside the same chunk must still zero-fill the gap.
	f, err := fsys.OpenFile("/small.txt", os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("X"), 20)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	data, err = fsys.ReadFile("/small.txt")
	require.NoError(t, err)
	require.Len(t, data, 21)
	require.Equal(t, "0123456789", string(data[:10]))
	require.Equal(t, make([]byte, 10), data[10:20])
	require.Equal(t, byte('X'), data[20])
}

// TestDeleteChunksMany verifies that rewriting and deleting a file with many
// chunks removes every chunk exactly once. Chunk keys are deleted while the
// cursor is iterating the bucket, so this guards that pattern against leaving
// chunks behind.
func TestDeleteChunksMany(t *testing.T) {
	store, db := chunkedStore(t, []byte("ts"), 61, 8)
	fsys, err := store.Open("fs")
	require.NoError(t, err)

	// 32 chunks of 8 bytes each.
	content := ""
	for i := 0; i < 32; i++ {
		content += "abcdefgh"
	}
	writeTestFile(t, fsys, "/big", content)

	node, err := fsys.resolve("/big")
	require.NoError(t, err)
	require.Len(t, chunkEntries(t, db, []byte("ts"), "fs"), 32)

	// Rewriting the whole file deletes all 32 chunks while iterating.
	require.NoError(t, fsys.db.Update(func(tx *bolt.Tx) error {
		b, err := fsys.openFsBucketForWrite(tx)
		if err != nil {
			return err
		}
		return b.writeAllChunks(node.Ino, []byte("short"), 8)
	}))
	require.Len(t, chunkEntries(t, db, []byte("ts"), "fs"), 1)

	// Deleting the inode releases the remaining chunk.
	require.NoError(t, fsys.db.Update(func(tx *bolt.Tx) error {
		b, err := fsys.openFsBucketForWrite(tx)
		if err != nil {
			return err
		}
		return b.deleteAllChunks(node.Ino)
	}))
	require.Empty(t, chunkEntries(t, db, []byte("ts"), "fs"))
}

// TestFullChunkOverwriteFastPath verifies that a write replacing a whole chunk
// produces the same bytes as a partial write, and does not leave extra chunks
// behind.
func TestFullChunkOverwriteFastPath(t *testing.T) {
	store, db := chunkedStore(t, []byte("ts"), 61, 8)
	fsys, err := store.Open("fs")
	require.NoError(t, err)

	writeTestFile(t, fsys, "/f", "0123456789ABCDEF") // two 8-byte chunks
	node, err := fsys.resolve("/f")
	require.NoError(t, err)
	require.Len(t, chunkEntries(t, db, []byte("ts"), "fs"), 2)

	// Overwrite the first chunk exactly.
	require.NoError(t, fsys.writeRange(node.Ino, 0, []byte("abcdefgh")))

	got, err := fsys.readRange(node.Ino, 0, 16)
	require.NoError(t, err)
	require.Equal(t, "abcdefgh89ABCDEF", string(got))
	require.Len(t, chunkEntries(t, db, []byte("ts"), "fs"), 2)

	// Overwrite the second chunk exactly as well.
	require.NoError(t, fsys.writeRange(node.Ino, 8, []byte("IJKLMNOP")))

	got, err = fsys.readRange(node.Ino, 0, 16)
	require.NoError(t, err)
	require.Equal(t, "abcdefghIJKLMNOP", string(got))
	require.Len(t, chunkEntries(t, db, []byte("ts"), "fs"), 2)
}
