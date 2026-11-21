package fs

import (
	"context"
	"testing"
	"time"

	"github.com/absfs/absfs"
	bolt "go.etcd.io/bbolt"
)

// openFSForTest opens a path-backed file system through a Store. The helper
// owns the bbolt database and closes it during test cleanup.
func openFSForTest(tb testing.TB, path, bucketpath string) (*FileSystem, error) {
	tb.Helper()

	db, err := bolt.Open(path, 0644, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	tb.Cleanup(func() {
		_ = db.Close()
	})

	store, err := Open(context.Background(), db, nil)
	if err != nil {
		return nil, err
	}

	return store.Open(bucketpath)
}

func openFSWithContentForTest(tb testing.TB, path, bucketpath string, contentFS absfs.FileSystem) (*FileSystem, error) {
	tb.Helper()

	filesystem, err := openFSForTest(tb, path, bucketpath)
	if err != nil {
		return nil, err
	}
	filesystem.SetContentFS(contentFS)
	return filesystem, nil
}

func newFSWithContentForTest(db *bolt.DB, bucketpath string, contentFS absfs.FileSystem) (*FileSystem, error) {
	filesystem, err := newFS(db, bucketpath)
	if err != nil {
		return nil, err
	}
	filesystem.SetContentFS(contentFS)
	return filesystem, nil
}
