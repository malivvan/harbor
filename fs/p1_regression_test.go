package fs

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// A failed chunked write must not advance the handle offset or the in-memory
// size: a retry would otherwise write at the wrong position and leave a hole.
func TestChunkedWriteFailureKeepsOffset(t *testing.T) {
	store, db := openEncStore(t, []byte("ts"), Config{Secret: testEncKey(31), ChunkSize: 8})
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/f", "abc")

	f, err := fsys.OpenFile("/f", os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Seek(3, io.SeekStart)
	require.NoError(t, err)

	// Make the next transaction fail.
	require.NoError(t, db.Close())

	if _, err := f.Write([]byte("XY")); err == nil {
		t.Fatal("Write succeeded although the database is closed")
	}

	pos, err := f.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(3), pos, "offset advanced on a failed write")
}

// A truncated dirent value must return a corrupt-data error instead of
// panicking in binary.BigEndian.Uint64.
func TestCorruptDirentValueReturnsError(t *testing.T) {
	store, db := openEncStore(t, []byte("ts"), Config{})
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	ino := fsys.rootIno

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, fsPath([]byte("ts"), "fs"))
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		return b.dirents.Put(dirKey(ino, "bad"), []byte{1, 2, 3})
	}))

	err = db.View(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, fsPath([]byte("ts"), "fs"))
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		node := newInode(os.ModeDir | 0755)
		return b.decodeDirents(ino, node)
	})
	require.Error(t, err, "expected a corrupt-data error for the truncated entry")
}
