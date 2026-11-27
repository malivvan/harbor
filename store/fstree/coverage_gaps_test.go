package fstree

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	hstore "github.com/malivvan/eidos-db/store"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func openTestFS(t *testing.T) *FS {
	t.Helper()
	store, _ := openEncStore(t, []byte(hstore.SERIES), Config{})
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	return fsys
}

func TestSubFSWrappers(t *testing.T) {
	fsys := openTestFS(t)
	require.NoError(t, fsys.Mkdir("/dir", 0755))
	writeTestFile(t, fsys, "/dir/a.txt", "hello")

	sub := &subFS{fs: fsys, dir: "/dir"}
	require.Equal(t, "/dir/a.txt", sub.fullPath("a.txt"))
	require.Equal(t, "/dir/a.txt", sub.fullPath("/../a.txt"), "fullPath must stay inside the subtree")

	fi, err := sub.Stat("a.txt")
	require.NoError(t, err)
	require.Equal(t, "a.txt", fi.Name())

	data, err := sub.ReadFile("a.txt")
	require.NoError(t, err)
	require.Equal(t, "hello", string(data))

	entries, err := sub.ReadDir("/")
	require.NoError(t, err)
	require.Len(t, entries, 1)

	require.NoError(t, sub.Mkdir("d2", 0755))
	require.NoError(t, sub.Rename("a.txt", "b.txt"))
	require.NoError(t, sub.Chmod("b.txt", 0600))
	now := time.Now()
	require.NoError(t, sub.Chtimes("b.txt", now, now))
	require.NoError(t, sub.Chown("b.txt", os.Getuid(), os.Getgid()))

	f, err := sub.OpenFile("b.txt", os.O_RDWR, 0600)
	require.NoError(t, err)
	_, err = f.WriteString("-more")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	nested, err := sub.Sub("d2")
	require.NoError(t, err)
	require.NotNil(t, nested)
	subEntries, err := sub.ReadDir("d2")
	require.NoError(t, err)
	require.Empty(t, subEntries)

	_, err = sub.Sub("missing")
	require.Error(t, err)
	writeTestFile(t, fsys, "/dir/plain", "x")
	_, err = sub.Sub("plain")
	require.Error(t, err)

	require.NoError(t, sub.Remove("b.txt"))
	require.NoError(t, sub.Remove("plain"))
}

func TestFileInfoAndDirEntryHelpers(t *testing.T) {
	fsys := openTestFS(t)
	require.NoError(t, fsys.Mkdir("/dir", 0755))
	writeTestFile(t, fsys, "/dir/f", "abc")

	fi, err := fsys.Stat("/dir/f")
	require.NoError(t, err)
	require.False(t, fi.IsDir())
	require.Equal(t, int64(3), fi.Size())
	require.NotNil(t, fi.Sys())
	_, ok := fi.Sys().(*iNode)
	require.True(t, ok)

	dirInfo, err := fsys.Stat("/dir")
	require.NoError(t, err)
	require.True(t, dirInfo.IsDir())

	openFile, err := fsys.Open("/dir/f")
	require.NoError(t, err)
	fileInfo, err := openFile.Stat()
	require.NoError(t, err)
	require.False(t, fileInfo.(*fileinfo).IsDir())
	require.NotNil(t, fileInfo.(*fileinfo).Sys())
	require.NoError(t, openFile.Close())

	// fakeDirEntry is used by FastWalk callbacks.
	d := &fakeDirEntry{name: "x", isDir: true}
	require.Equal(t, "x", d.Name())
	require.True(t, d.IsDir())
	require.Equal(t, fs.ModeDir, d.Type())
	_, err = d.Info()
	require.Error(t, err)

	stub := &fakeDirEntry{name: "y", info: fi}
	info, err := stub.Info()
	require.NoError(t, err)
	require.Equal(t, "f", info.Name())
	require.Equal(t, os.FileMode(0), stub.Type())
}

func TestInodeStringAndSortHelpers(t *testing.T) {
	e1 := &entry{Name: "b", Ino: 2}
	e2 := &entry{Name: "a", Ino: 1}
	require.Equal(t, `"b":2`, e1.String())

	list := entries{e1, e2}
	require.Equal(t, `entries["b":2,"a":1]`, list.String())
	require.Equal(t, 2, list.Len())
	require.False(t, list.Less(0, 1))
	list.Swap(0, 1)
	require.True(t, list.Less(0, 1))

	names := nameSorter([]os.FileInfo{
		&fileinfo{name: "b", node: newInode(0)},
		&fileinfo{name: "a", node: newInode(0)},
	})
	require.Equal(t, 2, names.Len())
	require.False(t, names.Less(0, 1))
	names.Swap(0, 1)
	require.True(t, names.Less(0, 1))
}

func TestFastWalkHelpers(t *testing.T) {
	require.False(t, isPermissionError(nil))
	require.True(t, isPermissionError(os.ErrPermission))
	require.True(t, isPermissionError(errors.New("access is denied")))
	require.False(t, isPermissionError(errors.New("boom")))
}

func TestAEADEnabledHelper(t *testing.T) {
	var e *encState
	require.False(t, e.enabled())
	e = &encState{}
	require.True(t, e.enabled())
}

func TestChunkFullFileHelpers(t *testing.T) {
	store, _ := chunkedStore(t, []byte(hstore.SERIES), 61, 8)
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/file", "0123456789abc")

	node, err := fsys.resolve("/file")
	require.NoError(t, err)

	require.NoError(t, fsys.db.View(func(tx *bolt.Tx) error {
		b, err := fsys.openFsBucket(tx)
		require.NoError(t, err)
		data, err := b.readAllChunks(node.Ino)
		require.NoError(t, err)
		require.Equal(t, "0123456789abc", string(data))
		return nil
	}))

	require.NoError(t, fsys.db.Update(func(tx *bolt.Tx) error {
		b, err := fsys.openFsBucketForWrite(tx)
		require.NoError(t, err)
		return b.writeAllChunks(node.Ino, []byte("abcdefghij"), 8)
	}))

	require.NoError(t, fsys.db.View(func(tx *bolt.Tx) error {
		b, err := fsys.openFsBucket(tx)
		require.NoError(t, err)
		data, err := b.readAllChunks(node.Ino)
		require.NoError(t, err)
		require.Equal(t, "abcdefghij", string(data))
		return nil
	}))

	// Partial range write and truncation through the FS wrappers.
	require.NoError(t, fsys.writeRange(node.Ino, 2, []byte("XY")))
	got, err := fsys.readRange(node.Ino, 0, 4)
	require.NoError(t, err)
	require.Equal(t, "abXY", string(got))

	require.NoError(t, fsys.truncateRange(node.Ino, 5))
	got, err = fsys.readRange(node.Ino, 0, 8)
	require.NoError(t, err)
	require.Equal(t, "abXYe\x00\x00\x00", string(got))

	// Whole-file helpers.
	require.NoError(t, fsys.saveChunkedData(node.Ino, []byte("zzzz")))
	got, err = fsys.loadChunkedData(node.Ino)
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(got, []byte("zzzz")))

	var nilBucket *fsBucket
	data, err := nilBucket.readAllChunks(1)
	require.NoError(t, err)
	require.Nil(t, data)
	require.ErrorIs(t, nilBucket.writeAllChunks(1, nil, 8), os.ErrInvalid)
	require.ErrorIs(t, nilBucket.writeChunk(1, 0, []byte("x")), os.ErrInvalid)
	got, err = nilBucket.readChunk(1, 0)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestSnapshotFSWrappers(t *testing.T) {
	fsys := openTestFS(t)
	require.NoError(t, fsys.Mkdir("/dir", 0755))
	writeTestFile(t, fsys, "/dir/f.txt", "snapshot")

	snap, err := fsys.CreateSnapshot("s")
	require.NoError(t, err)
	defer snap.Release()

	require.ErrorIs(t, snap.Chdir("/dir"), os.ErrPermission)
	wd, err := snap.Getwd()
	require.NoError(t, err)
	require.Equal(t, "/", wd)
	require.NotEmpty(t, snap.TempDir())

	_, err = snap.Create("/x")
	require.ErrorIs(t, err, os.ErrPermission)
	err = snap.Mkdir("/x", 0755)
	require.ErrorIs(t, err, os.ErrPermission)
	require.ErrorIs(t, snap.MkdirAll("/x", 0755), os.ErrPermission)
	require.ErrorIs(t, snap.Remove("/x"), os.ErrPermission)
	require.ErrorIs(t, snap.RemoveAll("/x"), os.ErrPermission)
	require.ErrorIs(t, snap.Truncate("/x", 0), os.ErrPermission)
	require.ErrorIs(t, snap.Chmod("/x", 0600), os.ErrPermission)
	require.ErrorIs(t, snap.Chown("/x", 0, 0), os.ErrPermission)
	require.ErrorIs(t, snap.Chtimes("/x", time.Now(), time.Now()), os.ErrPermission)
	require.ErrorIs(t, snap.Rename("/x", "/y"), os.ErrPermission)

	f, err := snap.OpenFile("/dir/f.txt", os.O_RDONLY, 0)
	require.NoError(t, err)
	require.Equal(t, "snapshot", string(mustReadAll(t, f)))
	require.NoError(t, f.Close())

	_, err = snap.OpenFile("/dir/f.txt", os.O_WRONLY, 0)
	require.ErrorIs(t, err, os.ErrPermission)

	sub, err := snap.Sub("/dir")
	require.NoError(t, err)
	entries, err := fs.ReadDir(sub, ".")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	data, err := fs.ReadFile(sub, "f.txt")
	require.NoError(t, err)
	require.Equal(t, "snapshot", string(data))

	_, err = snap.Sub("/dir/f.txt")
	require.Error(t, err)

	// snapshotSubFS wrappers.
	ss := &snapshotSubFS{snapshot: snap, dir: "/dir"}
	require.Equal(t, "/dir/f.txt", ss.fullPath("f.txt"))
	_, err = ss.OpenFile("f.txt", os.O_RDONLY, 0)
	require.NoError(t, err)
	require.ErrorIs(t, ss.Mkdir("x", 0755), os.ErrPermission)
	require.ErrorIs(t, ss.Remove("x"), os.ErrPermission)
	require.ErrorIs(t, ss.Rename("x", "y"), os.ErrPermission)
	require.ErrorIs(t, ss.Chmod("x", 0600), os.ErrPermission)
	require.ErrorIs(t, ss.Chtimes("x", time.Now(), time.Now()), os.ErrPermission)
	require.ErrorIs(t, ss.Chown("x", 0, 0), os.ErrPermission)
	_, err = ss.Stat("f.txt")
	require.NoError(t, err)
	_, err = ss.ReadDir("/")
	require.NoError(t, err)
	_, err = ss.ReadFile("f.txt")
	require.NoError(t, err)
	_, err = ss.Sub("missing")
	require.Error(t, err)
	_, err = ss.Sub("f.txt")
	require.Error(t, err)
}

func mustReadAll(t *testing.T, f interface{ Read([]byte) (int, error) }) []byte {
	t.Helper()
	var out []byte
	buf := make([]byte, 4)
	for {
		n, err := f.Read(buf)
		out = append(out, buf[:n]...)
		if err == io.EOF {
			return out
		}
		require.NoError(t, err)
	}
}

func TestSnapshotFileMethods(t *testing.T) {
	fsys := openTestFS(t)
	require.NoError(t, fsys.Mkdir("/dir", 0755))
	writeTestFile(t, fsys, "/dir/f.txt", "0123456789")
	writeTestFile(t, fsys, "/dir/g.txt", "g")

	snap, err := fsys.CreateSnapshot("s")
	require.NoError(t, err)
	defer snap.Release()

	fi, err := snap.Open("/dir/f.txt")
	require.NoError(t, err)
	f := fi.(*snapshotFile)
	require.Equal(t, "/dir/f.txt", f.Name())

	buf := make([]byte, 4)
	n, err := f.ReadAt(buf, 2)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, "2345", string(buf))

	n, err = f.ReadAt(buf, 9)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 1, n)

	_, err = f.ReadAt(buf, -1)
	require.Error(t, err)

	for _, tc := range []struct {
		off, want int64
		err       bool
	}{
		{0, 0, false},
		{2, 2, false},
		{0, 10, false},
	} {
		if tc.off == 0 && tc.want == 10 {
			got, err := f.Seek(0, io.SeekEnd)
			require.NoError(t, err)
			require.Equal(t, int64(10), got)
			continue
		}
		got, err := f.Seek(tc.off, io.SeekStart)
		require.Equal(t, tc.err, err != nil)
		require.Equal(t, tc.want, got)
	}
	_, err = f.Seek(0, 99)
	require.Error(t, err)
	_, err = f.Seek(-100, io.SeekStart)
	require.Error(t, err)

	require.ErrorIs(t, f.Truncate(0), os.ErrPermission)
	_, err = f.Write([]byte("x"))
	require.ErrorIs(t, err, os.ErrPermission)
	_, err = f.WriteAt([]byte("x"), 0)
	require.ErrorIs(t, err, os.ErrPermission)
	_, err = f.WriteString("x")
	require.ErrorIs(t, err, os.ErrPermission)
	require.NoError(t, f.Sync())
	_, err = f.Stat()
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Directory files expose Readdir/ReadDir/Readdirnames.
	di, err := snap.Open("/dir")
	require.NoError(t, err)
	dir := di.(*snapshotFile)
	_, err = dir.Read(buf)
	require.True(t, errors.Is(err, syscall.EISDIR))
	infos, err := dir.Readdir(1)
	require.NoError(t, err)
	require.Len(t, infos, 1)
	infos, err = dir.Readdir(-1)
	require.NoError(t, err)
	require.Len(t, infos, 1)
	_, err = dir.Readdir(1)
	require.Equal(t, io.EOF, err)

	dir2, err := snap.Open("/dir")
	require.NoError(t, err)
	d := dir2.(*snapshotFile)
	entries, err := d.ReadDir(1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	_, err = d.ReadDir(1)
	require.NoError(t, err)
	_, err = d.ReadDir(1)
	require.Equal(t, io.EOF, err)

	dir3, err := snap.Open("/dir")
	require.NoError(t, err)
	d3 := dir3.(*snapshotFile)
	names, err := d3.Readdirnames(-1)
	require.NoError(t, err)
	require.Len(t, names, 2)

	// A released snapshot closes its files.
	rel, err := fsys.CreateSnapshot("r")
	require.NoError(t, err)
	rf, err := rel.Open("/dir/f.txt")
	require.NoError(t, err)
	require.NoError(t, rel.Release())
	_, err = rf.Read(buf)
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = rf.ReadAt(buf, 0)
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = rf.Seek(0, io.SeekStart)
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = rf.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = rf.Readdir(-1)
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = rf.ReadDir(-1)
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = rf.Readdirnames(-1)
	require.ErrorIs(t, err, os.ErrClosed)
	require.ErrorIs(t, rel.Chdir("/"), os.ErrPermission)
	_, err = rel.Getwd()
	require.NoError(t, err)
	_, err = rel.Open("/x")
	require.Error(t, err)
}

func TestSnapshotCopyAndManager(t *testing.T) {
	// Copying out of a snapshot writes to the same database while the
	// snapshot's read transaction is open. Pre-size the mmap so the write
	// never needs to remap (which bbolt would block on until the snapshot
	// closes).
	db, err := bolt.Open(filepath.Join(t.TempDir(), "copy.db"), 0600, &bolt.Options{
		Timeout:         time.Second,
		InitialMmapSize: 64 << 20,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	store, err := Open(context.Background(), db, Config{Root: [][]byte{[]byte(hstore.SERIES)}})
	require.NoError(t, err)
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	require.NoError(t, fsys.Mkdir("/src", 0755))
	writeTestFile(t, fsys, "/src/a.txt", "A")
	writeTestFile(t, fsys, "/src/b.txt", "B")

	snap, err := fsys.CreateSnapshot("s")
	require.NoError(t, err)
	defer snap.Release()

	require.NoError(t, snap.CopyToFS("/src/a.txt", "/copied.txt"))
	data, err := fsys.ReadFile("/copied.txt")
	require.NoError(t, err)
	require.Equal(t, "A", string(data))

	require.NoError(t, snap.CopyToFS("/src", "/copied-dir"))
	data, err = fsys.ReadFile("/copied-dir/b.txt")
	require.NoError(t, err)
	require.Equal(t, "B", string(data))

	dirInfo, err := fsys.Stat("/copied-dir")
	require.NoError(t, err)
	require.True(t, dirInfo.IsDir())

	// CopyToFS on a released snapshot.
	rel, err := fsys.CreateSnapshot("rel")
	require.NoError(t, err)
	require.NoError(t, rel.Release())
	require.ErrorIs(t, rel.CopyToFS("/src/a.txt", "/x"), os.ErrClosed)

	mgr := fsys.NewSnapshotManager()
	s1, err := mgr.Create("one")
	require.NoError(t, err)
	_, err = mgr.Create("one")
	require.ErrorIs(t, err, os.ErrExist)
	got, ok := mgr.Get("one")
	require.True(t, ok)
	require.Same(t, s1, got)
	require.Equal(t, []string{"one"}, mgr.List())
	require.NoError(t, mgr.Delete("one"))
	require.ErrorIs(t, mgr.Delete("one"), os.ErrNotExist)
}

func TestSymlinkAndLchown(t *testing.T) {
	fsys := openTestFS(t)
	writeTestFile(t, fsys, "/target.txt", "target")

	require.NoError(t, fsys.Symlink("/target.txt", "/link"))
	link, err := fsys.Readlink("/link")
	require.NoError(t, err)
	require.Equal(t, "/target.txt", link)

	// Following the link works.
	data, err := fsys.ReadFile("/link")
	require.NoError(t, err)
	require.Equal(t, "target", string(data))

	// Lstat does not follow it.
	info, err := fsys.Lstat("/link")
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&os.ModeSymlink)

	require.NoError(t, fsys.Lchown("/link", os.Getuid(), os.Getgid()))
	require.Error(t, fsys.Lchown("/missing", 0, 0))
	require.NoError(t, fsys.Lchown("/target.txt", os.Getuid(), os.Getgid()))

	_, err = fsys.Readlink("/target.txt")
	require.Error(t, err)
	_, err = fsys.Readlink("/missing")
	require.Error(t, err)

	// A symlink cycle is rejected by the depth bound.
	require.NoError(t, fsys.Symlink("/loop2", "/loop1"))
	require.NoError(t, fsys.Symlink("/loop1", "/loop2"))
	_, err = fsys.ReadFile("/loop1")
	require.Error(t, err)
}

func TestFileErrorBranches(t *testing.T) {
	fsys := openTestFS(t)
	writeTestFile(t, fsys, "/f", "data")

	f, err := fsys.OpenFile("/f", os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.Read(make([]byte, 1))
	require.Error(t, err)
	require.NoError(t, f.Close())
	_, err = f.Read(make([]byte, 1))
	require.ErrorIs(t, err, os.ErrClosed)

	r, err := fsys.OpenFile("/f", os.O_RDONLY, 0)
	require.NoError(t, err)
	_, err = r.Write([]byte("x"))
	require.Error(t, err)
	require.NoError(t, r.Close())

	dir, err := fsys.OpenFile("/", os.O_RDONLY, 0)
	require.NoError(t, err)
	_, err = dir.Read(make([]byte, 1))
	require.Error(t, err)
	_, err = dir.Seek(0, 99)
	require.Error(t, err)
	require.NoError(t, dir.Close())

	require.NoError(t, fsys.Chmod("/f", 0444))
	_, err = fsys.OpenFile("/f", os.O_WRONLY, 0)
	require.Error(t, err)
	require.NoError(t, fsys.Chmod("/f", 0644))

	// WriteAt bounds.
	wr, err := fsys.OpenFile("/f", os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = wr.WriteAt(make([]byte, 4), -1)
	require.Error(t, err)
	_, err = wr.WriteAt(make([]byte, 4), maxFileSize)
	require.Error(t, err)
	require.NoError(t, wr.Close())

	// Readdir on a regular file.
	rf, err := fsys.Open("/f")
	require.NoError(t, err)
	_, err = rf.Readdir(-1)
	require.Error(t, err)
	_, err = rf.ReadDir(-1)
	require.Error(t, err)
	_, err = rf.Readdirnames(-1)
	require.Error(t, err)
	require.NoError(t, rf.Close())

	// Empty and partial directory listings.
	dirFile, err := fsys.Open("/")
	require.NoError(t, err)
	infos, err := dirFile.Readdir(0)
	require.NoError(t, err)
	require.NotEmpty(t, infos)
	names, err := dirFile.Readdirnames(0)
	require.NoError(t, err)
	require.Empty(t, names)
	require.NoError(t, dirFile.Close())
}

func TestInoToPath(t *testing.T) {
	require.Equal(t, "/00/0000000000000001", path.Clean(inoToPath(1)))
	require.Equal(t, "/ff/ffffffffffffffff", path.Clean(inoToPath(^uint64(0))))
}

func TestInodeGobRoundTrip(t *testing.T) {
	n := newInode(os.ModeDir | 0755)
	n.Ino = 7
	n.Nlink = 2
	n.Size = 42
	n.Uid = 1
	n.Gid = 2
	n.SetMtime(time.Unix(100, 0))
	n.Children = entries{{Name: "a", Ino: 1}}

	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(n))
	var out iNode
	require.NoError(t, gob.NewDecoder(&buf).Decode(&out))
	require.Equal(t, uint64(7), out.Ino)
	require.Equal(t, uint64(2), out.Nlink)
	require.Equal(t, int64(42), out.Size)
	require.Len(t, out.Children, 1)
}

func TestFSEncStateAccessor(t *testing.T) {
	plain := openTestFS(t)
	require.Nil(t, plain.encState())

	store, _ := openEncStore(t, []byte(hstore.SERIES), Config{Secret: testEncKey(81), EncryptContent: true, Root: [][]byte{[]byte(hstore.SERIES)}})
	encrypted, err := store.Open("fs")
	require.NoError(t, err)
	require.NotNil(t, encrypted.encState())
}
