package fstree

import (
	"errors"
	"io"
	"os"
	"syscall"
	"testing"
	"time"

	hstore "github.com/malivvan/eidos-db/store"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestCopyFileDirectoryAndSymlink(t *testing.T) {
	fsys := openTestFS(t)
	require.NoError(t, fsys.Mkdir("/src", 0755))
	require.NoError(t, fsys.Mkdir("/src/sub", 0755))
	writeTestFile(t, fsys, "/src/a.txt", "A")
	writeTestFile(t, fsys, "/src/sub/b.txt", "B")
	writeTestFile(t, fsys, "/target", "T")
	require.NoError(t, fsys.Symlink("/target", "/src/link"))

	// File copy, then mutate the copy: the source must be independent.
	require.NoError(t, fsys.Copy("/src/a.txt", "/a-copy.txt"))
	data, err := fsys.ReadFile("/a-copy.txt")
	require.NoError(t, err)
	require.Equal(t, "A", string(data))

	f, err := fsys.OpenFile("/a-copy.txt", os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("Z"), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	orig, err := fsys.ReadFile("/src/a.txt")
	require.NoError(t, err)
	require.Equal(t, "A", string(orig))

	// Directory copy is recursive and copies symlinks as symlinks.
	require.NoError(t, fsys.Copy("/src", "/src-copy"))
	data, err = fsys.ReadFile("/src-copy/sub/b.txt")
	require.NoError(t, err)
	require.Equal(t, "B", string(data))
	info, err := fsys.Lstat("/src-copy/link")
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&os.ModeSymlink)
	link, err := fsys.Readlink("/src-copy/link")
	require.NoError(t, err)
	require.Equal(t, "/target", link)
	data, err = fsys.ReadFile("/src-copy/link")
	require.NoError(t, err)
	require.Equal(t, "T", string(data))

	// Copying a symlink itself preserves its target.
	require.NoError(t, fsys.Copy("/src/link", "/link-copy"))
	info, err = fsys.Lstat("/link-copy")
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&os.ModeSymlink)
	link, err = fsys.Readlink("/link-copy")
	require.NoError(t, err)
	require.Equal(t, "/target", link)

	// Copying an empty directory works.
	require.NoError(t, fsys.Mkdir("/empty", 0755))
	require.NoError(t, fsys.Copy("/empty", "/empty-copy"))
	entries, err := fsys.ReadDir("/empty-copy")
	require.NoError(t, err)
	require.Empty(t, entries)

	// Error branches.
	require.Error(t, fsys.Copy("/", "/x"))
	require.ErrorIs(t, fsys.Copy("/missing", "/x"), os.ErrNotExist)
	require.ErrorIs(t, fsys.Copy("/src/a.txt", "/missing/x"), os.ErrNotExist)
	require.ErrorIs(t, fsys.Copy("/src/a.txt", "/src/a.txt"), os.ErrExist)
	require.Error(t, fsys.Copy("/src", "/src/sub/inside"))
	require.Error(t, fsys.Copy("/src", "/src-copy"))
}

func TestCopyWithContentFS(t *testing.T) {
	fsys := openTestFS(t)
	fsys.SetContentFS(newContentStorageFS(t).AsContentFS())
	writeTestFile(t, fsys, "/external.txt", "external")
	require.NoError(t, fsys.Mkdir("/dir", 0755))
	writeTestFile(t, fsys, "/dir/child", "child")

	require.NoError(t, fsys.Copy("/external.txt", "/copy.txt"))
	data, err := fsys.ReadFile("/copy.txt")
	require.NoError(t, err)
	require.Equal(t, "external", string(data))

	require.NoError(t, fsys.Copy("/dir", "/dir-copy"))
	data, err = fsys.ReadFile("/dir-copy/child")
	require.NoError(t, err)
	require.Equal(t, "child", string(data))

	// Removing content-backed files removes the external objects too.
	require.NoError(t, fsys.Remove("/copy.txt"))
	_, err = fsys.ReadFile("/copy.txt")
	require.Error(t, err)
	require.NoError(t, fsys.RemoveAll("/dir-copy"))
}

func TestRenameEdges(t *testing.T) {
	fsys := openTestFS(t)
	require.NoError(t, fsys.Mkdir("/dir", 0755))
	require.NoError(t, fsys.Mkdir("/dir/sub", 0755))
	writeTestFile(t, fsys, "/file", "one")
	writeTestFile(t, fsys, "/other", "two")
	writeTestFile(t, fsys, "/dir/inner", "inner")

	// Error branches.
	require.Error(t, fsys.Rename("/", "/x"))
	require.ErrorIs(t, fsys.Rename("/missing", "/x"), os.ErrNotExist)
	require.ErrorIs(t, fsys.Rename("/missing/file", "/x"), os.ErrNotExist)
	require.ErrorIs(t, fsys.Rename("/file", "/missing/x"), os.ErrNotExist)
	require.True(t, errors.Is(fsys.Rename("/file", "/file"), syscall.EINVAL))
	require.True(t, errors.Is(fsys.Rename("/dir", "/dir/sub"), syscall.EINVAL))
	require.True(t, errors.Is(fsys.Rename("/dir", "/dir"), syscall.EINVAL))

	// File over file replaces the destination.
	require.NoError(t, fsys.Rename("/file", "/other"))
	data, err := fsys.ReadFile("/other")
	require.NoError(t, err)
	require.Equal(t, "one", string(data))
	_, err = fsys.Stat("/file")
	require.Error(t, err)

	// File over directory and directory over file are rejected.
	writeTestFile(t, fsys, "/file2", "x")
	require.True(t, errors.Is(fsys.Rename("/file2", "/dir"), syscall.EISDIR))
	require.True(t, errors.Is(fsys.Rename("/dir", "/other"), syscall.EEXIST))

	// Cross-directory move.
	require.NoError(t, fsys.Rename("/other", "/dir/moved"))
	data, err = fsys.ReadFile("/dir/moved")
	require.NoError(t, err)
	require.Equal(t, "one", string(data))

	// Same-directory rename.
	require.NoError(t, fsys.Rename("/dir/inner", "/dir/renamed"))
	data, err = fsys.ReadFile("/dir/renamed")
	require.NoError(t, err)
	require.Equal(t, "inner", string(data))

	// Renaming a symlink moves the link, not its target.
	writeTestFile(t, fsys, "/target", "target")
	require.NoError(t, fsys.Symlink("/target", "/link"))
	require.NoError(t, fsys.Rename("/link", "/link2"))
	link, err := fsys.Readlink("/link2")
	require.NoError(t, err)
	require.Equal(t, "/target", link)
	data, err = fsys.ReadFile("/target")
	require.NoError(t, err)
	require.Equal(t, "target", string(data))
}

func TestSaveParentChildReplacement(t *testing.T) {
	fsys := openTestFS(t)
	writeTestFile(t, fsys, "/a.txt", "old")

	parent, old := fsys.loadParentChild("/", "a.txt")
	require.NotNil(t, parent)
	require.NotNil(t, old)
	oldIno := old.Ino

	// Replacing a singly-linked child deletes the old inode.
	replacement := newInode(0644)
	require.NoError(t, fsys.saveParentChild(parent, "a.txt", replacement))

	_, err := fsys.loadInode(oldIno)
	require.Error(t, err)
	data, err := fsys.ReadFile("/a.txt")
	require.NoError(t, err)
	require.Empty(t, data)

	// A multiply-linked child survives with its link count decremented.
	writeTestFile(t, fsys, "/b.txt", "linked")
	parent, old = fsys.loadParentChild("/", "b.txt")
	require.NotNil(t, old)
	old.Nlink = 2
	_, err = fsys.saveInode(old)
	require.NoError(t, err)

	require.NoError(t, fsys.saveParentChild(parent, "b.txt", newInode(0644)))
	survivor, err := fsys.loadInode(old.Ino)
	require.NoError(t, err)
	require.Equal(t, uint64(1), survivor.Nlink)
}

func TestSnapshotReadAtBranches(t *testing.T) {
	fsys := openTestFS(t)
	writeTestFile(t, fsys, "/f", "0123456789")

	snap, err := fsys.CreateSnapshot("s")
	require.NoError(t, err)
	defer snap.Release()

	f, err := snap.Open("/f")
	require.NoError(t, err)
	sf := f.(*snapshotFile)

	buf := make([]byte, 4)
	n, err := sf.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, "0123", string(buf))

	n, err = sf.ReadAt(buf, 8)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 2, n)
	require.Equal(t, "89", string(buf[:2]))

	_, err = sf.ReadAt(buf, -1)
	require.Error(t, err)
	_, err = sf.ReadAt(buf, 100)
	require.ErrorIs(t, err, io.EOF)

	// Read after EOF and with a zero-length buffer.
	_, err = sf.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	_, err = sf.Read(buf)
	require.ErrorIs(t, err, io.EOF)

	// Readlink/ReadDir error branches.
	_, err = snap.Readlink("/f")
	require.Error(t, err)
	_, err = snap.Readlink("/missing")
	require.Error(t, err)
	_, err = snap.ReadDir("/f")
	require.Error(t, err)
	_, err = snap.ReadDir("/missing")
	require.Error(t, err)
}

func TestSnapshotChunkedReadAt(t *testing.T) {
	store, _ := chunkedStore(t, []byte(hstore.SERIES), 63, 8)
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/f", "0123456789abcdefghij")

	snap, err := fsys.CreateSnapshot("s")
	require.NoError(t, err)
	defer snap.Release()

	f, err := snap.Open("/f")
	require.NoError(t, err)
	sf := f.(*snapshotFile)

	// Crosses the 8-byte chunk boundary.
	buf := make([]byte, 6)
	n, err := sf.ReadAt(buf, 6)
	require.NoError(t, err)
	require.Equal(t, 6, n)
	require.Equal(t, "6789ab", string(buf))

	// Partial read at the end.
	buf = make([]byte, 6)
	n, err = sf.ReadAt(buf, 17)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 3, n)
	require.Equal(t, "hij", string(buf[:3]))

	n, err = sf.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 6, n)
}

func TestInodeEncodingErrorsAndStateHelpers(t *testing.T) {
	fsys := openTestFS(t)
	writeTestFile(t, fsys, "/f", "data")

	require.NoError(t, fsys.db.View(func(tx *bolt.Tx) error {
		b, err := fsys.openFsBucket(tx)
		require.NoError(t, err)

		_, err = b.GetInode(nilIno)
		require.ErrorIs(t, err, errInvalidIno)
		_, err = b.GetInode(9999)
		require.Error(t, err)
		require.ErrorIs(t, b.decodeNode(9999, new(iNode)), os.ErrNotExist)
		require.ErrorIs(t, b.decodeNode(1, nil), errNilNode)
		require.ErrorIs(t, b.encodeNode(1, nil), errNilNode)
		require.ErrorIs(t, b.Symlink(0, ""), errNilNode)
		require.Empty(t, b.Readlink(0))
		_, err = b.Get("missing")
		require.ErrorIs(t, err, os.ErrNotExist)
		return nil
	}))

	_, err := fsys.loadInode(0)
	require.ErrorIs(t, err, errNilIno)

	// State helpers and cache accessors.
	require.NoError(t, fsys.db.Update(func(tx *bolt.Tx) error {
		b, err := fsys.openFsBucket(tx)
		if err != nil {
			return err
		}
		require.NoError(t, b.Put("k", []byte("v")))
		got, err := b.Get("k")
		require.NoError(t, err)
		require.Equal(t, []byte("v"), got)
		return nil
	}))

	require.Equal(t, "/tmp", (&FS{}).TempDir())
	require.Equal(t, os.FileMode(0022), (&FS{}).Umask())
	require.Equal(t, CacheStats{}, (&FS{}).CacheStats())
	(&FS{}).FlushCache()
	(&FS{}).SetCacheSize(10)
}

func TestSaveInodeAndDataHelpers(t *testing.T) {
	fsys := openTestFS(t)
	node := newInode(0644)
	ino, err := fsys.saveInodeAndData(node, []byte("payload"))
	require.NoError(t, err)
	require.NotZero(t, ino)

	data, err := fsys.loadData(ino)
	require.NoError(t, err)
	require.Equal(t, "payload", string(data))

	// Zero-size truncation deletes the data key.
	node.Size = 0
	require.NoError(t, fsys.deleteDataAndSaveInode(node))
	data, err = fsys.loadData(ino)
	require.NoError(t, err)
	require.Empty(t, data)
}

func TestIsDescendantPath(t *testing.T) {
	require.True(t, isDescendantPath("/a", "/a"))
	require.True(t, isDescendantPath("/a", "/a/b"))
	require.False(t, isDescendantPath("/a", "/ab"))
	require.True(t, isDescendantPath("/", "/anything"))
	require.False(t, isDescendantPath("/a/b", "/a"))
}

func TestDirectoryListingBoundaries(t *testing.T) {
	fsys := openTestFS(t)
	require.NoError(t, fsys.Mkdir("/d", 0755))
	for _, name := range []string{"a", "b", "c"} {
		writeTestFile(t, fsys, "/d/"+name, name)
	}

	f, err := fsys.OpenFile("/d", os.O_RDONLY, 0)
	require.NoError(t, err)
	infos, err := f.Readdir(5)
	require.NoError(t, err)
	require.Len(t, infos, 3)
	infos, err = f.Readdir(1)
	require.ErrorIs(t, err, io.EOF)
	require.Empty(t, infos)
	require.NoError(t, f.Close())

	f, err = fsys.OpenFile("/d", os.O_RDONLY, 0)
	require.NoError(t, err)
	names, err := f.Readdirnames(2)
	require.NoError(t, err)
	require.Len(t, names, 2)
	names, err = f.Readdirnames(2)
	require.NoError(t, err)
	require.Len(t, names, 1)
	_, err = f.Readdirnames(2)
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, f.Close())

	f, err = fsys.OpenFile("/d", os.O_RDONLY, 0)
	require.NoError(t, err)
	entries, err := f.ReadDir(2)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	entries, err = f.ReadDir(-1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	_, err = f.ReadDir(2)
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, f.Close())

	// Reading a directory and changing times through the file handle.
	f, err = fsys.OpenFile("/d", os.O_RDONLY, 0)
	require.NoError(t, err)
	_, err = f.Read(make([]byte, 1))
	require.Error(t, err)
	require.NoError(t, f.Close())
}

func TestFilesystemTimeAndTempdir(t *testing.T) {
	fsys := openTestFS(t)
	require.Equal(t, "/tmp", fsys.TempDir())
	fsys.SetTempdir("/custom")
	require.Equal(t, "/custom", fsys.TempDir())
	fsys.SetTempdir("/")

	fsys.SetUmask(0077)
	require.Equal(t, os.FileMode(0077), fsys.Umask())
	fsys.SetUmask(0)
	require.Equal(t, os.FileMode(0022), fsys.Umask())

	fsys.SetContentFS(nil)
	_ = time.Now()
}

func TestChunkedWriteAndTruncateTransactions(t *testing.T) {
	store, _ := chunkedStore(t, []byte(hstore.SERIES), 64, 8)
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/f", "0123456789")

	node, err := fsys.resolve("/f")
	require.NoError(t, err)

	require.NoError(t, fsys.writeRangeAndInode(node.Ino, 0, []byte("AB"), node))
	data, err := fsys.readRange(node.Ino, 0, 4)
	require.NoError(t, err)
	require.Equal(t, "AB23", string(data))

	node.Size = 2
	require.NoError(t, fsys.truncateRangeAndInode(node.Ino, 2, node))
	data, err = fsys.loadChunkedData(node.Ino)
	require.NoError(t, err)
	require.Equal(t, []byte("AB"), data)
}
