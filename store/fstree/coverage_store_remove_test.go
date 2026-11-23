package fstree

import (
	"context"
	"errors"
	"io"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestRemoveBranches(t *testing.T) {
	fsys := openTestFS(t)
	require.NoError(t, fsys.Mkdir("/dir", 0755))
	writeTestFile(t, fsys, "/dir/child", "c")
	writeTestFile(t, fsys, "/file", "f")

	require.True(t, errors.Is(fsys.Remove("/"), syscall.EBUSY))
	require.ErrorIs(t, fsys.Remove("/missing"), os.ErrNotExist)
	require.True(t, errors.Is(fsys.Remove("/dir"), syscall.ENOTEMPTY))
	require.NoError(t, fsys.Remove("/dir/child"))
	require.NoError(t, fsys.Remove("/dir"))

	// A multiply-linked inode survives with a decremented count.
	writeTestFile(t, fsys, "/linked", "x")
	node, err := fsys.resolve("/linked")
	require.NoError(t, err)
	node.Nlink = 2
	_, err = fsys.saveInode(node)
	require.NoError(t, err)
	require.NoError(t, fsys.Remove("/linked"))
	survivor, err := fsys.loadInode(node.Ino)
	require.NoError(t, err)
	require.Equal(t, uint64(1), survivor.Nlink)

	// Missing parent directory.
	require.ErrorIs(t, fsys.Remove("/missing/file"), os.ErrNotExist)

	// Symlinks are removed as links.
	writeTestFile(t, fsys, "/target", "t")
	require.NoError(t, fsys.Symlink("/target", "/link"))
	require.NoError(t, fsys.Remove("/link"))
	_, err = fsys.ReadFile("/target")
	require.NoError(t, err)
}

func TestMkdirBranches(t *testing.T) {
	fsys := openTestFS(t)
	require.ErrorIs(t, fsys.Mkdir("/missing/sub", 0755), os.ErrNotExist)
	writeTestFile(t, fsys, "/file", "f")
	require.ErrorIs(t, fsys.Mkdir("/file", 0755), os.ErrExist)
	require.NoError(t, fsys.Mkdir("/dir", 0755))
	require.ErrorIs(t, fsys.Mkdir("/dir", 0755), os.ErrExist)
	require.NoError(t, fsys.Mkdir("/dir/sub", 0755))
}

func TestSymlinkBranches(t *testing.T) {
	fsys := openTestFS(t)
	writeTestFile(t, fsys, "/target", "t")
	require.NoError(t, fsys.Symlink("/target", "/link"))

	// Existing destination.
	require.ErrorIs(t, fsys.Symlink("/target", "/link"), os.ErrExist)
	// Missing parent.
	require.ErrorIs(t, fsys.Symlink("/target", "/missing/link"), os.ErrNotExist)

	// A missing target creates a dangling link.
	require.NoError(t, fsys.Symlink("/missing", "/dangling"))
	target, err := fsys.Readlink("/dangling")
	require.NoError(t, err)
	require.Equal(t, "/missing", target)
	_, err = fsys.ReadFile("/dangling")
	require.Error(t, err)

	// Relative targets resolve against the link's directory.
	require.NoError(t, fsys.Mkdir("/dir", 0755))
	writeTestFile(t, fsys, "/dir/rel-target", "rel")
	require.NoError(t, fsys.Symlink("rel-target", "/dir/rel-link"))
	data, err := fsys.ReadFile("/dir/rel-link")
	require.NoError(t, err)
	require.Equal(t, "rel", string(data))
}

func TestChownBranches(t *testing.T) {
	fsys := openTestFS(t)
	require.ErrorIs(t, fsys.Chown("/missing", 0, 0), os.ErrNotExist)

	writeTestFile(t, fsys, "/target", "t")
	require.NoError(t, fsys.Symlink("/target", "/link"))
	require.NoError(t, fsys.Chown("/link", os.Getuid(), os.Getgid()))

	require.NoError(t, fsys.Mkdir("/dir", 0755))
	writeTestFile(t, fsys, "/dir/rel", "x")
	require.NoError(t, fsys.Symlink("rel", "/dir/rel-link"))
	require.NoError(t, fsys.Chown("/dir/rel-link", os.Getuid(), os.Getgid()))

	// Chowning a dangling symlink fails; Lchown still updates the link.
	require.NoError(t, fsys.Symlink("/missing", "/broken"))
	require.Error(t, fsys.Chown("/broken", 0, 0))
	require.NoError(t, fsys.Lchown("/broken", 0, 0))
}

func TestSaveDataBranches(t *testing.T) {
	fsys := openTestFS(t)
	err := fsys.saveData(0, []byte("x"))
	require.ErrorIs(t, err, errNilIno)
	_, err = fsys.loadData(0)
	require.ErrorIs(t, err, errNilIno)

	node := newInode(0644)
	ino, err := fsys.saveInode(node)
	require.NoError(t, err)
	require.NoError(t, fsys.saveData(ino, []byte("bbolt")))
	data, err := fsys.loadData(ino)
	require.NoError(t, err)
	require.Equal(t, "bbolt", string(data))

	// Missing data decodes to an empty file.
	node2 := newInode(0644)
	ino2, err := fsys.saveInode(node2)
	require.NoError(t, err)
	data, err = fsys.loadData(ino2)
	require.NoError(t, err)
	require.Empty(t, data)

	// Content filesystem storage.
	content := newContentStorageFS(t)
	fsys.SetContentFS(content.AsContentFS())
	require.NoError(t, fsys.saveData(ino, []byte("external")))
	data, err = fsys.loadData(ino)
	require.NoError(t, err)
	require.Equal(t, "external", string(data))

	// Chunked storage.
	chunked, _ := chunkedStore(t, []byte("ts"), 66, 8)
	chunkedFS, err := chunked.Open("fs")
	require.NoError(t, err)
	cnode := newInode(0644)
	cino, err := chunkedFS.saveInode(cnode)
	require.NoError(t, err)
	require.NoError(t, chunkedFS.saveData(cino, []byte("chunked-data")))
	cnode.Size = int64(len("chunked-data"))
	_, err = chunkedFS.saveInode(cnode)
	require.NoError(t, err)
	data, err = chunkedFS.loadData(cino)
	require.NoError(t, err)
	require.Equal(t, "chunked-data", string(data))
}

func TestCreateSnapshotError(t *testing.T) {
	store, db := openEncStore(t, []byte("ts"), Config{})
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	_, err = fsys.CreateSnapshot("s")
	require.Error(t, err)
}

func TestStoreHelpers(t *testing.T) {
	store, _ := openEncStore(t, []byte("root"), Config{})
	require.Equal(t, [][]byte{[]byte("root")}, store.Root())

	root := store.Root()
	root[0][0] = 'X'
	require.Equal(t, byte('r'), store.Root()[0][0])

	var nilStore *Store
	require.Nil(t, nilStore.Root())
	require.ErrorIs(t, nilStore.Close(), ErrNilParam)
	_, err := nilStore.Open("x")
	require.ErrorIs(t, err, ErrNilParam)
	_, err = nilStore.OpenWithContentFS("x", nil)
	require.ErrorIs(t, err, ErrNilParam)
	require.ErrorIs(t, nilStore.DeleteFS("x"), ErrNilParam)

	_, err = store.Open("a/b")
	require.ErrorIs(t, err, ErrNilParam)

	fsys, err := store.OpenWithContentFS("actual", newContentStorageFS(t).AsContentFS())
	require.NoError(t, err)
	require.NotNil(t, fsys)

	names, err := store.FileSystems()
	require.NoError(t, err)
	require.Contains(t, names, "actual")

	// A plaintext filesystem has no user data until it holds a file.
	require.NoError(t, store.checkMode("actual"))
	has, err := store.hasUserData(store.bucketPath("actual"))
	require.NoError(t, err)
	require.False(t, has)

	f, err := fsys.OpenFile("/file", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	has, err = store.hasUserData(store.bucketPath("actual"))
	require.NoError(t, err)
	require.True(t, has)

	require.ErrorIs(t, store.DeleteFS(""), ErrNilParam)
	require.ErrorIs(t, store.DeleteFS("missing"), os.ErrNotExist)
	require.NoError(t, store.DeleteFS("actual"))

	// DeleteFS on a store whose root bucket disappeared.
	emptyStore, _ := openEncStore(t, []byte("ghost"), Config{})
	require.ErrorIs(t, emptyStore.DeleteFS("x"), os.ErrNotExist)
}

func TestVerifyAll(t *testing.T) {
	ctx := context.Background()

	// Plaintext filesystems verify successfully.
	plain, _ := openEncStore(t, []byte("ts"), Config{})
	plainFS, err := plain.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, plainFS, "/f", "data")
	require.NoError(t, plain.VerifyAll(ctx))

	// A corrupted encrypted inode fails verification.
	enc, encDB := openEncStore(t, []byte("ts"), Config{Secret: testEncKey(82), EncryptMetadata: true, EncryptContent: true})
	encFS, err := enc.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, encFS, "/f", "data")
	require.NoError(t, enc.VerifyAll(ctx))

	require.NoError(t, encDB.Update(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, encFS.bucket)
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		c := b.inodes.Cursor()
		key, value := c.First()
		for key != nil && len(key) != 8 {
			key, value = c.Next()
		}
		require.NotNil(t, key)
		corrupt := append([]byte(nil), value...)
		corrupt[len(corrupt)-1] ^= 0xff
		return b.inodes.Put(key, corrupt)
	}))
	require.Error(t, enc.VerifyAll(ctx))
}

func TestSnapshotReleaseIdempotent(t *testing.T) {
	fsys := openTestFS(t)
	snap, err := fsys.CreateSnapshot("s")
	require.NoError(t, err)
	require.NoError(t, snap.Release())
	require.NoError(t, snap.Release())

	_, err = snap.Open("/f")
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = snap.ReadFile("/f")
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = snap.Stat("/f")
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = snap.ReadDir("/")
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = snap.Sub("/")
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = snap.Readlink("/")
	require.ErrorIs(t, err, os.ErrClosed)
}

func TestNewFSRejectsCorruptRootInode(t *testing.T) {
	store, db := openEncStore(t, []byte("ts"), Config{})
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/f", "data")

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, fsys.bucket)
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		return b.inodes.Put(i2b(fsys.rootIno), []byte("garbage"))
	}))

	_, err = store.Open("fs")
	require.Error(t, err)
}

func TestHasUserDataKinds(t *testing.T) {
	store, _ := openEncStore(t, []byte("ts"), Config{})
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	path := store.bucketPath("fs")

	has, err := store.hasUserData(path)
	require.NoError(t, err)
	require.False(t, has)

	// A symlink alone counts as user data.
	require.NoError(t, fsys.Symlink("/target", "/link"))
	has, err = store.hasUserData(path)
	require.NoError(t, err)
	require.True(t, has)

	// Content in chunks counts as user data.
	chunkStore, _ := chunkedStore(t, []byte("ts"), 67, 8)
	chunkFS, err := chunkStore.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, chunkFS, "/f", "0123456789")
	has, err = chunkStore.hasUserData(chunkStore.bucketPath("fs"))
	require.NoError(t, err)
	require.True(t, has)

	// Content in the data bucket counts as user data.
	dataStore, _ := openEncStore(t, []byte("ts"), Config{})
	dataFS, err := dataStore.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, dataFS, "/f", "data")
	has, err = dataStore.hasUserData(dataStore.bucketPath("fs"))
	require.NoError(t, err)
	require.True(t, has)

	_, err = store.OpenWithContentFS("a/b", nil)
	require.ErrorIs(t, err, ErrNilParam)
}

func TestSnapshotListingBoundaries(t *testing.T) {
	fsys := openTestFS(t)
	require.NoError(t, fsys.Mkdir("/dir", 0755))
	writeTestFile(t, fsys, "/dir/a", "a")
	writeTestFile(t, fsys, "/dir/b", "b")
	require.NoError(t, fsys.Mkdir("/empty", 0755))

	snap, err := fsys.CreateSnapshot("s")
	require.NoError(t, err)
	defer snap.Release()

	entries, err := snap.ReadDir("/dir")
	require.NoError(t, err)
	require.Len(t, entries, 2)

	entries, err = snap.ReadDir("/empty")
	require.NoError(t, err)
	require.Empty(t, entries)

	f, err := snap.Open("/dir")
	require.NoError(t, err)
	sf := f.(*snapshotFile)
	infos, err := sf.Readdir(2)
	require.NoError(t, err)
	require.Len(t, infos, 2)
	names, err := sf.Readdirnames(-1)
	require.NoError(t, err)
	require.Empty(t, names)

	f, err = snap.Open("/empty")
	require.NoError(t, err)
	empty := f.(*snapshotFile)
	infos, err = empty.Readdir(-1)
	require.NoError(t, err)
	require.Empty(t, infos)
	_, err = empty.Readdir(1)
	require.ErrorIs(t, err, io.EOF)
}
