package fstree

import (
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/malivvan/tempest/internal/bucket"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestFileAfterCloseReturnsErrClosed(t *testing.T) {
	fsys, err := openFSForTest(t, filepath.Join(t.TempDir(), "closed.db"), "")
	require.NoError(t, err)

	f, err := fsys.Create("/file")
	require.NoError(t, err)
	_, err = f.Write([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = f.Read(make([]byte, 1))
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = f.Seek(0, io.SeekStart)
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = f.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
	require.ErrorIs(t, f.Truncate(1), os.ErrClosed)
	require.NoError(t, f.Close(), "Close must be idempotent")
}

func TestFileSizeLimits(t *testing.T) {
	fsys, err := openFSForTest(t, filepath.Join(t.TempDir(), "size.db"), "")
	require.NoError(t, err)
	writeStoreFile(t, fsys, "/f", "abc")

	err = fsys.Truncate("/f", maxFileSize+1)
	require.ErrorIs(t, err, syscall.EFBIG)

	f, err := fsys.OpenFile("/f", os.O_RDWR, 0644)
	require.NoError(t, err)
	_, err = f.Seek(maxFileSize, io.SeekStart)
	require.NoError(t, err)
	_, err = f.Write([]byte("x"))
	require.ErrorIs(t, err, syscall.EFBIG)
}

func TestReadAtPreservesOffset(t *testing.T) {
	fsys, err := openFSForTest(t, filepath.Join(t.TempDir(), "readat.db"), "")
	require.NoError(t, err)
	writeStoreFile(t, fsys, "/f", "0123456789")

	f, err := fsys.OpenFile("/f", os.O_RDWR, 0644)
	require.NoError(t, err)
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)

	buf := make([]byte, 3)
	_, err = f.ReadAt(buf, 5)
	require.NoError(t, err)
	require.Equal(t, "567", string(buf))

	pos, err := f.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	require.Zero(t, pos, "ReadAt must not move the sequential offset")
}

func TestReadDirZeroAtEOF(t *testing.T) {
	fsys, err := openFSForTest(t, filepath.Join(t.TempDir(), "rd.db"), "")
	require.NoError(t, err)
	writeStoreFile(t, fsys, "/f", "x")

	dir, err := fsys.Open("/")
	require.NoError(t, err)
	_, err = dir.ReadDir(-1)
	require.NoError(t, err)
	entries, err := dir.ReadDir(0)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestMkdirAllRejectsExistingFile(t *testing.T) {
	fsys, err := openFSForTest(t, filepath.Join(t.TempDir(), "mk.db"), "")
	require.NoError(t, err)
	writeStoreFile(t, fsys, "/file", "x")

	err = fsys.MkdirAll("/file", 0755)
	require.ErrorIs(t, err, syscall.ENOTDIR)
}

func TestEncryptedEmptyCiphertextIsRejected(t *testing.T) {
	root := []byte(bucket.SERIES)
	store, db := openEncStore(t, root, Config{Secret: testEncKey(90), EncryptContent: true})
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeStoreFile(t, fsys, "/secret", "sensitive")

	info, err := fsys.Stat("/secret")
	require.NoError(t, err)
	ino := info.Sys().(*iNode).Ino

	// Replace the sealed value with an empty one: it cannot be valid ciphertext.
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, fsPath(root, "fs"))
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		return b.data.Put(i2b(ino), []byte{})
	}))

	_, err = fsys.ReadFile("/secret")
	require.ErrorIs(t, err, ErrAuthFailed)
}

var _ = path.Clean
var _ = errors.Is
