package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/malivvan/storm/fs/absfs"
	bolt "go.etcd.io/bbolt"
)

func openStoreTestDB(t *testing.T) *bolt.DB {
	t.Helper()

	db, err := bolt.Open(filepath.Join(t.TempDir(), "store.db"), 0644, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open bbolt: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func writeStoreFile(t *testing.T, filesystem *FileSystem, name, content string) {
	t.Helper()

	file, err := filesystem.Create(name)
	if err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	if _, err := file.Write([]byte(content)); err != nil {
		_ = file.Close()
		t.Fatalf("write %s: %v", name, err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close %s: %v", name, err)
	}
}

func assertFSBucket(t *testing.T, db *bolt.DB, parts ...string) {
	t.Helper()

	err := db.View(func(tx *bolt.Tx) error {
		var bucket *bolt.Bucket
		for _, part := range parts {
			if bucket == nil {
				bucket = tx.Bucket([]byte(part))
			} else {
				bucket = bucket.Bucket([]byte(part))
			}
			if bucket == nil {
				t.Fatalf("bucket %v not found", parts)
			}
		}
		for _, name := range buckets {
			if bucket.Bucket([]byte(name)) == nil {
				t.Fatalf("bucket %v is missing %q", parts, name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("inspect buckets: %v", err)
	}
}

func TestStoreOpen(t *testing.T) {
	ctx := context.Background()

	store, err := Open(nil, nil, nil)
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("expected ErrNilContext, got %v", err)
	}
	if store != nil {
		t.Fatal("expected nil store for nil context")
	}

	db := openStoreTestDB(t)
	store, err = Open(ctx, nil, nil)
	if !errors.Is(err, ErrNilParam) {
		t.Fatalf("expected ErrNilParam, got %v", err)
	}
	if store != nil {
		t.Fatal("expected nil store for nil database")
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	store, err = Open(canceled, db, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if store != nil {
		t.Fatal("expected nil store for canceled context")
	}
}

func TestStoreOpenAndDelete(t *testing.T) {
	db := openStoreTestDB(t)
	root := []byte("tenant")
	store, err := Open(context.Background(), db, root)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	if !reflect.DeepEqual(root, store.Root()) {
		t.Fatalf("unexpected root: %q", store.Root())
	}
	root[0] = 'X'
	if !reflect.DeepEqual([]byte("tenant"), store.Root()) {
		t.Fatal("Store.Root must return a copy")
	}
	if store.NativeDB() != db {
		t.Fatal("NativeDB must return the supplied database")
	}

	docs, err := store.Open("docs")
	if err != nil {
		t.Fatalf("new docs fs: %v", err)
	}
	writeStoreFile(t, docs, "readme.txt", "docs")

	media, err := store.Open("media")
	if err != nil {
		t.Fatalf("new media fs: %v", err)
	}
	writeStoreFile(t, media, "readme.txt", "media")

	assertFSBucket(t, db, "tenant", "docs")
	assertFSBucket(t, db, "tenant", "media")

	names, err := store.FileSystems()
	if err != nil {
		t.Fatalf("list file systems: %v", err)
	}
	if !reflect.DeepEqual(names, []string{"docs", "media"}) {
		t.Fatalf("unexpected file systems: %v", names)
	}

	content, err := docs.ReadFile("readme.txt")
	if err != nil {
		t.Fatalf("read docs file: %v", err)
	}
	if string(content) != "docs" {
		t.Fatalf("unexpected docs content: %q", content)
	}

	// A Store-managed file system must not close the shared database.
	if err := docs.Close(); err != nil {
		t.Fatalf("close docs fs: %v", err)
	}
	if err := db.View(func(tx *bolt.Tx) error { return nil }); err != nil {
		t.Fatalf("database was closed by FileSystem.Close: %v", err)
	}

	if err := store.DeleteFS("docs"); err != nil {
		t.Fatalf("delete docs fs: %v", err)
	}
	names, err = store.FileSystems()
	if err != nil {
		t.Fatalf("list file systems after delete: %v", err)
	}
	if !reflect.DeepEqual(names, []string{"media"}) {
		t.Fatalf("unexpected file systems after delete: %v", names)
	}
	if err := store.DeleteFS("docs"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
	if err := store.DeleteFS(""); !errors.Is(err, ErrNilParam) {
		t.Fatalf("expected ErrNilParam, got %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
}

func TestStoreRootsAreIsolated(t *testing.T) {
	db := openStoreTestDB(t)
	storeA, err := Open(context.Background(), db, []byte("a"))
	if err != nil {
		t.Fatalf("open store a: %v", err)
	}
	storeB, err := Open(context.Background(), db, []byte("b"))
	if err != nil {
		t.Fatalf("open store b: %v", err)
	}

	fsA, err := storeA.Open("docs")
	if err != nil {
		t.Fatalf("new fs a: %v", err)
	}
	fsB, err := storeB.Open("docs")
	if err != nil {
		t.Fatalf("new fs b: %v", err)
	}
	writeStoreFile(t, fsA, "value.txt", "a")
	writeStoreFile(t, fsB, "value.txt", "b")

	content, err := fsA.ReadFile("value.txt")
	if err != nil || string(content) != "a" {
		t.Fatalf("unexpected fs a content: %q, %v", content, err)
	}
	content, err = fsB.ReadFile("value.txt")
	if err != nil || string(content) != "b" {
		t.Fatalf("unexpected fs b content: %q, %v", content, err)
	}

	assertFSBucket(t, db, "a", "docs")
	assertFSBucket(t, db, "b", "docs")
}

func TestStoreWithoutRoot(t *testing.T) {
	db := openStoreTestDB(t)
	store, err := Open(context.Background(), db, nil)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	filesystem, err := store.Open("docs")
	if err != nil {
		t.Fatalf("new fs: %v", err)
	}
	writeStoreFile(t, filesystem, "readme.txt", "top-level")

	assertFSBucket(t, db, "docs")
	names, err := store.FileSystems()
	if err != nil {
		t.Fatalf("list file systems: %v", err)
	}
	if !reflect.DeepEqual(names, []string{"docs"}) {
		t.Fatalf("unexpected file systems: %v", names)
	}
}

// TestFileSystemWithExternalContentFS tests using an external filesystem for content storage
func TestFileSystemWithExternalContentFS(t *testing.T) {
	tmpfile := filepath.Join(t.TempDir(), "test.db")

	contentFS := newContentStorageFS(t)

	// Open filesystem with external content storage
	fs, err := openFSWithContentForTest(t, tmpfile, "", contentFS)
	if err != nil {
		t.Fatalf("OpenWithContentFS failed: %v", err)
	}
	defer fs.Close()

	// Create a test file
	f, err := fs.OpenFile("/test.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	// Write data
	testData := []byte("hello external storage!")
	n, err := f.Write(testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Write wrote %d bytes, want %d", n, len(testData))
	}
	f.Close()

	// Read the file back
	f, err = fs.OpenFile("/test.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile for read failed: %v", err)
	}

	buf := make([]byte, len(testData))
	n, err = f.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Read %d bytes, want %d", n, len(testData))
	}
	if string(buf) != string(testData) {
		t.Fatalf("Read wrong data: got %q, want %q", buf, testData)
	}
	f.Close()

	// Verify content is stored in the external filesystem, not BoltDB
	// Get the inode number
	info, err := fs.Stat("/test.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	node, ok := info.Sys().(*iNode)
	if !ok {
		t.Fatal("Failed to get inode")
	}

	// Check that content exists in the external filesystem
	contentPath := inoToPath(node.Ino)
	_, err = contentFS.Stat(contentPath)
	if err != nil {
		t.Fatalf("Content not found in the external filesystem: %v", err)
	}

	// Test file deletion
	err = fs.Remove("/test.txt")
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Verify file was deleted
	_, err = fs.Stat("/test.txt")
	if !os.IsNotExist(err) {
		t.Fatalf("File should not exist after remove, got error: %v", err)
	}

	// Verify content was deleted from the external filesystem
	_, err = contentFS.Stat(contentPath)
	if !os.IsNotExist(err) {
		t.Fatalf("Content should not exist in the external filesystem after remove, got error: %v", err)
	}
}

// TestFileSystemWithoutExternalStorage tests backward compatibility
func TestFileSystemWithoutExternalStorage(t *testing.T) {
	tmpfile := filepath.Join(t.TempDir(), "test.db")

	// Open filesystem without external storage (uses BoltDB by default)
	fs, err := openFSForTest(t, tmpfile, "")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer fs.Close()

	// Create a test file
	f, err := fs.OpenFile("/test.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	// Write data
	testData := []byte("hello boltdb storage!")
	n, err := f.Write(testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Write wrote %d bytes, want %d", n, len(testData))
	}
	f.Close()

	// Read the file back
	f, err = fs.OpenFile("/test.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile for read failed: %v", err)
	}

	buf := make([]byte, len(testData))
	n, err = f.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Read %d bytes, want %d", n, len(testData))
	}
	if string(buf) != string(testData) {
		t.Fatalf("Read wrong data: got %q, want %q", buf, testData)
	}
	f.Close()
}

// TestRemoveAllWithExternalStorage tests RemoveAll with an external content filesystem
func TestRemoveAllWithExternalStorage(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Skipping on Windows - Walk has path separator issues (uses filepath.Join which produces \\ on Windows, but BoltFS internally uses / separators)")
	}

	tmpfile := filepath.Join(t.TempDir(), "test.db")

	contentFS := newContentStorageFS(t)

	// Open filesystem with external content storage
	fs, err := openFSWithContentForTest(t, tmpfile, "", contentFS)
	if err != nil {
		t.Fatalf("OpenWithContentFS failed: %v", err)
	}
	defer fs.Close()

	// Create a directory with files
	err = fs.Mkdir("/testdir", 0755)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	// Create multiple files
	var inodes []uint64
	for i := 0; i < 5; i++ {
		filename := filepath.Join("/testdir", "file"+string(rune('0'+i))+".txt")
		f, err := fs.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			t.Fatalf("OpenFile failed: %v", err)
		}
		f.Write([]byte("test data " + string(rune('0'+i))))

		// Get inode number
		info, _ := fs.Stat(filename)
		if node, ok := info.Sys().(*iNode); ok {
			inodes = append(inodes, node.Ino)
		}

		f.Close()
	}

	// Remove all
	err = fs.RemoveAll("/testdir")
	if err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}

	// Verify directory was removed
	_, err = fs.Stat("/testdir")
	if err == nil {
		t.Fatal("Directory should not exist after RemoveAll")
	}
	if !os.IsNotExist(err) {
		t.Fatalf("Expected NotExist error, got: %v", err)
	}

	// Verify all content files were deleted from the external filesystem
	for _, ino := range inodes {
		contentPath := inoToPath(ino)
		_, err := contentFS.Stat(contentPath)
		if !os.IsNotExist(err) {
			t.Errorf("Content for inode %d should not exist in the external filesystem, got error: %v", ino, err)
		}
	}
}

// TestSetContentFS tests setting content filesystem on existing filesystem
func TestSetContentFS(t *testing.T) {
	tmpfile := filepath.Join(t.TempDir(), "test.db")

	// Open filesystem without external storage
	fs, err := openFSForTest(t, tmpfile, "")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer fs.Close()

	contentFS := newContentStorageFS(t)

	// Set content filesystem
	fs.SetContentFS(contentFS)

	// Create a test file
	f, err := fs.OpenFile("/test.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	testData := []byte("test data")
	f.Write(testData)
	f.Close()

	// Get inode number
	info, err := fs.Stat("/test.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	node, ok := info.Sys().(*iNode)
	if !ok {
		t.Fatal("Failed to get inode")
	}

	// Verify content exists in the external filesystem
	contentPath := inoToPath(node.Ino)
	_, err = contentFS.Stat(contentPath)
	if err != nil {
		t.Fatalf("Content not found in the external filesystem: %v", err)
	}
}

// TestOpenWithContentFS tests creating a filesystem with a content filesystem
func TestOpenWithContentFS(t *testing.T) {
	tmpfile := filepath.Join(t.TempDir(), "test.db")

	// Open BoltDB
	db, err := bolt.Open(tmpfile, 0644, nil)
	if err != nil {
		t.Fatalf("bolt.Open failed: %v", err)
	}
	defer db.Close()

	contentFS := newContentStorageFS(t)

	// Create filesystem with content filesystem
	fs, err := newFSWithContentForTest(db, "", contentFS)
	if err != nil {
		t.Fatalf("OpenWithContentFS failed: %v", err)
	}

	// Create a test file
	f, err := fs.OpenFile("/test.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	testData := []byte("test data")
	f.Write(testData)
	f.Close()

	// Get inode number
	info, err := fs.Stat("/test.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	node, ok := info.Sys().(*iNode)
	if !ok {
		t.Fatal("Failed to get inode")
	}

	// Verify content exists in the external filesystem
	contentPath := inoToPath(node.Ino)
	_, err = contentFS.Stat(contentPath)
	if err != nil {
		t.Fatalf("Content not found in the external filesystem: %v", err)
	}
}

// openFSForTest opens a path-backed file system through a Store. The helper
// owns the bbolt database and closes it during test cleanup.
// newContentStorageFS returns an in-repo filesystem to use as the external
// content storage of another filesystem.
func newContentStorageFS(t *testing.T) *FileSystem {
	t.Helper()

	filesystem, err := openFSForTest(t, filepath.Join(t.TempDir(), "content.db"), "")
	if err != nil {
		t.Fatalf("open content storage: %v", err)
	}
	t.Cleanup(func() { _ = filesystem.Close() })
	return filesystem
}

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
	filesystem, err := newFS(db, bucketpath, nil)
	if err != nil {
		return nil, err
	}
	filesystem.SetContentFS(contentFS)
	return filesystem, nil
}
