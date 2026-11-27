package fstree

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"testing"

	hstore "github.com/malivvan/eidos-db/store"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func direntCount(t *testing.T, db *bolt.DB, root []byte, ino uint64) int {
	t.Helper()
	count := 0
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, fsPath(root, "fs"))
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		if b.dirents == nil {
			return nil
		}
		prefix := dirPrefix(ino)
		c := b.dirents.Cursor()
		for key, _ := c.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, _ = c.Next() {
			count++
		}
		return nil
	}))
	return count
}

// TestDirentsStoredSeparately verifies that directory entries live in the
// dirents bucket and the inode blob no longer carries the children list.
func TestDirentsStoredSeparately(t *testing.T) {
	root := []byte(hstore.SERIES)
	store, db := openEncStore(t, root, Config{})
	fsys, err := store.Open("fs")
	require.NoError(t, err)

	require.NoError(t, fsys.Mkdir("/dir", 0755))
	for i := 0; i < 3; i++ {
		writeTestFile(t, fsys, fmt.Sprintf("/dir/f%d", i), "x")
	}

	node, err := fsys.resolve("/dir")
	require.NoError(t, err)
	require.Len(t, node.Children, 3)
	require.Equal(t, 3, direntCount(t, db, root, node.Ino))

	// The persisted inode blob must not duplicate the children list.
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, fsPath(root, "fs"))
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		raw := b.inodes.Get(i2b(node.Ino))
		require.NotNil(t, raw)
		var blob iNode
		require.NoError(t, gob.NewDecoder(bytes.NewReader(raw)).Decode(&blob))
		require.Empty(t, blob.Children, "children belong in the dirents bucket")
		return nil
	}))

	// Unlink removes exactly one key.
	require.NoError(t, fsys.Remove("/dir/f1"))
	require.Equal(t, 2, direntCount(t, db, root, node.Ino))
	entries, err := fsys.ReadDir("/dir")
	require.NoError(t, err)
	require.Len(t, entries, 2)

	// Rename moves a key without leaking the old one.
	require.NoError(t, fsys.Rename("/dir/f0", "/dir/g0"))
	require.Equal(t, 2, direntCount(t, db, root, node.Ino))
	_, err = fsys.Stat("/dir/g0")
	require.NoError(t, err)
	_, err = fsys.Stat("/dir/f0")
	require.Error(t, err)

	// Reopening reads the entries back from the dirents bucket.
	fsys.FlushCache()
	node, err = fsys.resolve("/dir")
	require.NoError(t, err)
	require.Len(t, node.Children, 2)
}

// TestDirentsLegacyBlobFallback verifies that an inode written before the
// dirents bucket existed still reads its children from the blob, and that the
// next mutation migrates it.
func TestDirentsLegacyBlobFallback(t *testing.T) {
	root := []byte(hstore.SERIES)
	store, db := openEncStore(t, root, Config{})
	fsys, err := store.Open("fs")
	require.NoError(t, err)

	require.NoError(t, fsys.Mkdir("/legacy", 0755))
	writeTestFile(t, fsys, "/legacy/a", "a")
	writeTestFile(t, fsys, "/legacy/b", "b")

	node, err := fsys.resolve("/legacy")
	require.NoError(t, err)
	ino := node.Ino

	// Rewrite the inode as a legacy blob and drop the separate keys.
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, fsPath(root, "fs"))
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(node); err != nil {
			return err
		}
		if err := b.inodes.Put(i2b(ino), buf.Bytes()); err != nil {
			return err
		}
		return b.deleteDirents(ino)
	}))
	fsys.FlushCache()
	require.Equal(t, 0, direntCount(t, db, root, ino))

	node, err = fsys.resolve("/legacy")
	require.NoError(t, err)
	require.Len(t, node.Children, 2, "legacy blob children must be readable")

	// A mutation migrates the directory to the dirents format.
	writeTestFile(t, fsys, "/legacy/c", "c")
	require.Equal(t, 3, direntCount(t, db, root, ino))
	fsys.FlushCache()
	node, err = fsys.resolve("/legacy")
	require.NoError(t, err)
	require.Len(t, node.Children, 3)
}
