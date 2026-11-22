package fs

import (
	"bytes"
	"context"
	"os"
	"path"
	"sync"
	"syscall"
	"testing"

	bolt "go.etcd.io/bbolt"
)

// countChunkKeys returns the number of chunk keys stored for ino.
func countChunkKeys(t *testing.T, db *bolt.DB, root []byte, name string, ino uint64) int {
	t.Helper()
	var n int
	err := db.View(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, fsPath(root, name))
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		if b.chunks == nil {
			return nil
		}
		prefix := chunkKey(ino, 0)[:8]
		c := b.chunks.Cursor()
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("count chunk keys: %v", err)
	}
	return n
}

// symlinkStored reports whether a symlink target is stored for ino.
func symlinkStored(t *testing.T, db *bolt.DB, root []byte, name string, ino uint64) bool {
	t.Helper()
	var found bool
	err := db.View(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, fsPath(root, name))
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		found = b.symlinks.Get(i2b(ino)) != nil
		return nil
	})
	if err != nil {
		t.Fatalf("symlink stored: %v", err)
	}
	return found
}

// --- High: File.Truncate must not zero existing data when growing ---

func TestFileTruncateGrowPreservesData(t *testing.T) {
	fsys, err := openFSForTest(t, path.Join(t.TempDir(), "trunc.db"), "")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fsys.Create("/grow.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(10); err != nil {
		t.Fatalf("Truncate grow: %v", err)
	}
	f.Close()

	data, err := fsys.ReadFile("/grow.txt")
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 10 {
		t.Fatalf("len = %d, want 10", len(data))
	}
	if string(data[:5]) != "hello" {
		t.Fatalf("existing data lost, got %q", string(data))
	}
	for i := 5; i < 10; i++ {
		if data[i] != 0 {
			t.Fatalf("byte %d = %d, want 0", i, data[i])
		}
	}
}

// --- High: OpenFile must not recurse forever on a symlink cycle ---

func TestOpenFileSymlinkCycleELOOP(t *testing.T) {
	fsys, err := openFSForTest(t, path.Join(t.TempDir(), "loop.db"), "")
	if err != nil {
		t.Fatal(err)
	}

	if err := fsys.Symlink("b", "/a"); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Symlink("a", "/b"); err != nil {
		t.Fatal(err)
	}

	_, err = fsys.Open("/a")
	if err == nil {
		t.Fatal("expected ELOOP error")
	}
	if !isErrno(err, syscall.ELOOP) {
		t.Fatalf("expected ELOOP, got %v", err)
	}
}

func isErrno(err error, target syscall.Errno) bool {
	for err != nil {
		if e, ok := err.(syscall.Errno); ok {
			return e == target
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// --- High: O_TRUNC must truncate chunked content ---

func TestOpenFileOTruncChunked(t *testing.T) {
	store, db := openEncStore(t, nil, Config{Secret: testEncKey(70), ChunkSize: 8})
	fsys, err := store.Open("fs")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fsys.Create("/data.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("0123456789ABCDEF")); err != nil {
		t.Fatal(err)
	}
	f.Close()

	info, err := fsys.Stat("/data.bin")
	if err != nil {
		t.Fatal(err)
	}
	ino := info.Sys().(*iNode).Ino
	if countChunkKeys(t, db, nil, "fs", ino) == 0 {
		t.Fatal("expected chunks before truncate")
	}

	f, err = fsys.OpenFile("/data.bin", os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	info, err = fsys.Stat("/data.bin")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("size = %d, want 0", info.Size())
	}
	data, err := fsys.ReadFile("/data.bin")
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Fatalf("content not truncated, got %q", string(data))
	}
	if countChunkKeys(t, db, nil, "fs", ino) != 0 {
		t.Fatal("stale chunks remain after O_TRUNC")
	}
}

// --- High: FS.Truncate must not corrupt encrypted files ---

func TestTruncateEncryptedFile(t *testing.T) {
	store, _ := openEncStore(t, nil, Config{Secret: testEncKey(71), EncryptContent: true})
	fsys, err := store.Open("fs")
	if err != nil {
		t.Fatal(err)
	}

	writeStoreFile(t, fsys, "/e.txt", "hello world")

	if err := fsys.Truncate("/e.txt", 5); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	data, err := fsys.ReadFile("/e.txt")
	if err != nil {
		t.Fatalf("ReadFile after truncate: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("got %q, want %q", string(data), "hello")
	}
}

// --- High: Copy must copy file content and directory children ---

func TestCopyCopiesContent(t *testing.T) {
	fsys, err := openFSForTest(t, path.Join(t.TempDir(), "copy.db"), "")
	if err != nil {
		t.Fatal(err)
	}

	writeStoreFile(t, fsys, "/src.txt", "content")
	if err := fsys.Copy("/src.txt", "/dst.txt"); err != nil {
		t.Fatalf("Copy file: %v", err)
	}
	data, err := fsys.ReadFile("/dst.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "content" {
		t.Fatalf("copied content = %q, want %q", string(data), "content")
	}

	// Removing the copy must not affect the source.
	if err := fsys.Remove("/dst.txt"); err != nil {
		t.Fatal(err)
	}
	data, err = fsys.ReadFile("/src.txt")
	if err != nil {
		t.Fatalf("source gone after removing copy: %v", err)
	}
	if string(data) != "content" {
		t.Fatalf("source content = %q", string(data))
	}
}

func TestCopyDirectoryDeep(t *testing.T) {
	fsys, err := openFSForTest(t, path.Join(t.TempDir(), "copydir.db"), "")
	if err != nil {
		t.Fatal(err)
	}

	if err := fsys.Mkdir("/d", 0755); err != nil {
		t.Fatal(err)
	}
	writeStoreFile(t, fsys, "/d/a.txt", "A")

	if err := fsys.Copy("/d", "/d2"); err != nil {
		t.Fatalf("Copy dir: %v", err)
	}

	data, err := fsys.ReadFile("/d2/a.txt")
	if err != nil {
		t.Fatalf("read copied child: %v", err)
	}
	if string(data) != "A" {
		t.Fatalf("copied child = %q, want %q", string(data), "A")
	}

	// Removing the source must not break the copy.
	if err := fsys.RemoveAll("/d"); err != nil {
		t.Fatal(err)
	}
	data, err = fsys.ReadFile("/d2/a.txt")
	if err != nil {
		t.Fatalf("copy lost after removing source: %v", err)
	}
	if string(data) != "A" {
		t.Fatalf("copied child = %q", string(data))
	}
}

// --- High: deleting an inode must release chunk and symlink data ---

func TestDeleteInodeReleasesChunksAndSymlinks(t *testing.T) {
	store, db := openEncStore(t, nil, Config{Secret: testEncKey(72), ChunkSize: 8})
	fsys, err := store.Open("fs")
	if err != nil {
		t.Fatal(err)
	}

	writeStoreFile(t, fsys, "/chunk.bin", "0123456789ABCDEF")
	info, err := fsys.Stat("/chunk.bin")
	if err != nil {
		t.Fatal(err)
	}
	chunkIno := info.Sys().(*iNode).Ino
	if countChunkKeys(t, db, nil, "fs", chunkIno) == 0 {
		t.Fatal("expected chunks")
	}

	if err := fsys.Remove("/chunk.bin"); err != nil {
		t.Fatal(err)
	}
	if countChunkKeys(t, db, nil, "fs", chunkIno) != 0 {
		t.Fatal("chunks leaked after Remove")
	}

	// Symlink targets are released on delete too.
	if err := fsys.Symlink("target", "/link"); err != nil {
		t.Fatal(err)
	}
	linfo, err := fsys.Lstat("/link")
	if err != nil {
		t.Fatal(err)
	}
	linkIno := linfo.Sys().(*iNode).Ino
	if !symlinkStored(t, db, nil, "fs", linkIno) {
		t.Fatal("expected stored symlink target")
	}
	if err := fsys.Remove("/link"); err != nil {
		t.Fatal(err)
	}
	if symlinkStored(t, db, nil, "fs", linkIno) {
		t.Fatal("symlink target leaked after Remove")
	}
}

func TestRemoveAllReleasesChunks(t *testing.T) {
	store, db := openEncStore(t, nil, Config{Secret: testEncKey(73), ChunkSize: 8})
	fsys, err := store.Open("fs")
	if err != nil {
		t.Fatal(err)
	}

	if err := fsys.Mkdir("/dir", 0755); err != nil {
		t.Fatal(err)
	}
	writeStoreFile(t, fsys, "/dir/f.bin", "0123456789ABCDEF")
	info, err := fsys.Stat("/dir/f.bin")
	if err != nil {
		t.Fatal(err)
	}
	ino := info.Sys().(*iNode).Ino

	if err := fsys.RemoveAll("/dir"); err != nil {
		t.Fatal(err)
	}
	if countChunkKeys(t, db, nil, "fs", ino) != 0 {
		t.Fatal("chunks leaked after RemoveAll")
	}
}

// --- High: snapshot reads must consult the external contentFS ---

func TestSnapshotReadsExternalContentFS(t *testing.T) {
	fsys, err := openFSWithContentForTest(t, path.Join(t.TempDir(), "snap.db"), "", newContentStorageFS(t))
	if err != nil {
		t.Fatal(err)
	}

	writeStoreFile(t, fsys, "/ext.txt", "external")

	snap, err := fsys.CreateSnapshot("s1")
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	data, err := snap.ReadFile("/ext.txt")
	if err != nil {
		t.Fatalf("snapshot ReadFile: %v", err)
	}
	if string(data) != "external" {
		t.Fatalf("snapshot content = %q, want %q", string(data), "external")
	}

	sf, err := snap.Open("/ext.txt")
	if err != nil {
		t.Fatalf("snapshot Open: %v", err)
	}
	buf := make([]byte, len("external"))
	n, err := sf.Read(buf)
	if err != nil {
		t.Fatalf("snapshot file Read: %v", err)
	}
	if string(buf[:n]) != "external" {
		t.Fatalf("snapshot file content = %q", string(buf[:n]))
	}
}

// --- Medium: O_TRUNC and Truncate must honour the external contentFS ---

func TestTruncateExternalContentFS(t *testing.T) {
	fsys, err := openFSWithContentForTest(t, path.Join(t.TempDir(), "ext.db"), "", newContentStorageFS(t))
	if err != nil {
		t.Fatal(err)
	}

	writeStoreFile(t, fsys, "/t.txt", "hello world")

	if err := fsys.Truncate("/t.txt", 5); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	data, err := fsys.ReadFile("/t.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Fatalf("truncated content = %q, want %q", string(data), "hello")
	}

	// O_TRUNC clears external content too.
	f, err := fsys.OpenFile("/t.txt", os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	data, err = fsys.ReadFile("/t.txt")
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Fatalf("O_TRUNC left content %q", string(data))
	}
}

// --- Medium: Chmod must preserve type bits ---

func TestChmodPreservesTypeBits(t *testing.T) {
	fsys, err := openFSForTest(t, path.Join(t.TempDir(), "chmod.db"), "")
	if err != nil {
		t.Fatal(err)
	}

	if err := fsys.Mkdir("/dir", 0755); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Chmod("/dir", 0644); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	info, err := fsys.Stat("/dir")
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatal("Chmod cleared the directory type bit")
	}
	if info.Mode().Perm() != 0644 {
		t.Fatalf("perm = %o, want 644", info.Mode().Perm())
	}
}

// --- Medium: Chown must resolve relative symlinks against the parent dir ---

func TestChownRelativeSymlink(t *testing.T) {
	fsys, err := openFSForTest(t, path.Join(t.TempDir(), "chown.db"), "")
	if err != nil {
		t.Fatal(err)
	}

	if err := fsys.Mkdir("/d", 0755); err != nil {
		t.Fatal(err)
	}
	writeStoreFile(t, fsys, "/d/target", "x")
	if err := fsys.Symlink("target", "/d/link"); err != nil {
		t.Fatal(err)
	}

	if err := fsys.Chown("/d/link", 1234, 5678); err != nil {
		t.Fatalf("Chown on relative symlink: %v", err)
	}

	info, err := fsys.Stat("/d/target")
	if err != nil {
		t.Fatal(err)
	}
	node := info.Sys().(*iNode)
	if node.Uid != 1234 || node.Gid != 5678 {
		t.Fatalf("target uid/gid = %d/%d, want 1234/5678", node.Uid, node.Gid)
	}
}

// --- Medium: O_APPEND must write at the end ---

func TestOpenFileAppendWritesAtEnd(t *testing.T) {
	fsys, err := openFSForTest(t, path.Join(t.TempDir(), "append.db"), "")
	if err != nil {
		t.Fatal(err)
	}

	writeStoreFile(t, fsys, "/a.txt", "abc")

	f, err := fsys.OpenFile("/a.txt", os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("de")); err != nil {
		t.Fatal(err)
	}
	f.Close()

	data, err := fsys.ReadFile("/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "abcde" {
		t.Fatalf("content = %q, want %q", string(data), "abcde")
	}
}

// --- Medium: RemoveAll of a missing path returns nil ---

func TestRemoveAllMissingPath(t *testing.T) {
	fsys, err := openFSForTest(t, path.Join(t.TempDir(), "rmall.db"), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.RemoveAll("/does/not/exist"); err != nil {
		t.Fatalf("RemoveAll missing path = %v, want nil", err)
	}
}

// --- Medium: negative sizes and offsets must error, not panic ---

func TestNegativeSizesAndOffsets(t *testing.T) {
	fsys, err := openFSForTest(t, path.Join(t.TempDir(), "neg.db"), "")
	if err != nil {
		t.Fatal(err)
	}
	writeStoreFile(t, fsys, "/f.txt", "abcdef")

	if err := fsys.Truncate("/f.txt", -1); err == nil {
		t.Error("Truncate(-1) should error")
	}

	f, err := fsys.OpenFile("/f.txt", os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if err := f.Truncate(-1); err == nil {
		t.Error("File.Truncate(-1) should error")
	}
	buf := make([]byte, 4)
	if _, err := f.ReadAt(buf, -1); err == nil {
		t.Error("ReadAt(-1) should error")
	}
	if _, err := f.WriteAt([]byte("x"), -1); err == nil {
		t.Error("WriteAt(-1) should error")
	}
}

// --- Medium: concurrent directory mutations must not lose updates ---

func TestConcurrentMkdirNoLostUpdates(t *testing.T) {
	fsys, err := openFSForTest(t, path.Join(t.TempDir(), "concurrent.db"), "")
	if err != nil {
		t.Fatal(err)
	}
	// Disable the inode cache so each load truly comes from the database.
	fsys.SetCacheSize(0)

	const n = 30
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			name := path.Join("/", "dir"+itoa(i))
			if err := fsys.Mkdir(name, 0755); err != nil {
				t.Errorf("Mkdir %s: %v", name, err)
			}
		}(i)
	}
	wg.Wait()

	entries, err := fsys.ReadDir("/")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != n {
		t.Fatalf("ReadDir returned %d entries, want %d (lost updates)", len(entries), n)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [8]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}

// --- Medium: key rotation must install the new encryption state ---

func TestRotationInstallsNewEncState(t *testing.T) {
	ctx := context.Background()
	store, _ := openEncStore(t, nil, Config{Secret: testEncKey(74), EncryptContent: true})
	fsys, err := store.Open("fs")
	if err != nil {
		t.Fatal(err)
	}
	writeStoreFile(t, fsys, "/r.txt", "rotate me")

	if err := store.RotateAll(ctx, testEncKey(75)); err != nil {
		t.Fatalf("RotateAll: %v", err)
	}

	if fsys.enc.Load() == nil {
		t.Fatal("file system lost its encryption state after rotation")
	}
	data, err := fsys.ReadFile("/r.txt")
	if err != nil {
		t.Fatalf("ReadFile after rotation: %v", err)
	}
	if string(data) != "rotate me" {
		t.Fatalf("content = %q", string(data))
	}
}

// snapshot with chunked + encrypted store still reads through readRange.
func TestSnapshotChunkedContent(t *testing.T) {
	store, _ := openEncStore(t, nil, Config{Secret: testEncKey(76), ChunkSize: 8})
	fsys, err := store.Open("fs")
	if err != nil {
		t.Fatal(err)
	}
	writeStoreFile(t, fsys, "/c.bin", "0123456789ABCDEF")

	snap, err := fsys.CreateSnapshot("s")
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	data, err := snap.ReadFile("/c.bin")
	if err != nil {
		t.Fatalf("snapshot ReadFile: %v", err)
	}
	if string(data) != "0123456789ABCDEF" {
		t.Fatalf("content = %q", string(data))
	}
}
