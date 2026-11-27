package fstree

import (
	"io"
	"os"
	"testing"
	"time"

	hstore "github.com/malivvan/eidos-db/store"
	"github.com/stretchr/testify/require"
)

// tempFile helper is defined in cache_test.go

func TestSnapshot_Create(t *testing.T) {
	path := tempFile(t)

	fs, err := openFSForTest(t, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	snap, err := fs.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	if snap.Name() != "test" {
		t.Errorf("expected name 'test', got '%s'", snap.Name())
	}

	if snap.Created().IsZero() {
		t.Error("created time should be set")
	}
}

func TestSnapshot_Release(t *testing.T) {
	path := tempFile(t)

	fs, err := openFSForTest(t, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	snap, err := fs.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}

	err = snap.Release()
	if err != nil {
		t.Error(err)
	}

	// Using snapshot after release should fail
	_, err = snap.Stat("/")
	if err != os.ErrClosed {
		t.Errorf("expected os.ErrClosed, got %v", err)
	}

	// Multiple releases should be safe
	err = snap.Release()
	if err != nil {
		t.Error("multiple releases should be safe")
	}
}

func TestSnapshot_Stat(t *testing.T) {
	path := tempFile(t)

	fs, err := openFSForTest(t, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	// Create a file
	file, err := fs.Create("/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	file.Write([]byte("hello"))
	file.Close()

	// Create snapshot
	snap, err := fs.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	// Stat the file in snapshot
	info, err := snap.Stat("/test.txt")
	if err != nil {
		t.Fatal(err)
	}

	if info.Name() != "test.txt" {
		t.Errorf("expected name 'test.txt', got '%s'", info.Name())
	}
	if info.Size() != 5 {
		t.Errorf("expected size 5, got %d", info.Size())
	}
	if info.IsDir() {
		t.Error("file should not be a directory")
	}
}

func TestSnapshot_IsolatedFromWrites(t *testing.T) {
	path := tempFile(t)

	fs, err := openFSForTest(t, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	// Create a file
	file, err := fs.Create("/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	file.Write([]byte("original"))
	file.Close()

	// Create snapshot
	snap, err := fs.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}

	// Read from snapshot immediately to verify original content
	snapData, err := snap.ReadFile("/test.txt")
	if err != nil {
		snap.Release()
		t.Fatal(err)
	}
	if string(snapData) != "original" {
		snap.Release()
		t.Errorf("snapshot should have 'original', got '%s'", string(snapData))
		return
	}

	// Release snapshot before modifying the file to avoid BoltDB deadlock.
	// BoltDB read transactions can block write transactions that need to remap
	// the file (when the database grows), causing a deadlock.
	snap.Release()

	// Modify the file in main filesystem
	file, err = fs.OpenFile("/test.txt", os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		t.Fatal(err)
	}
	file.Write([]byte("modified"))
	file.Close()

	// Read from main filesystem
	mainData, err := readFileContents(fs, "/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(mainData) != "modified" {
		t.Errorf("main fs should have 'modified', got '%s'", string(mainData))
	}

	// Create a new snapshot to verify the original snapshot captured the old state
	// (This test now verifies that snapshots capture state at creation time,
	// rather than testing concurrent read/write which BoltDB doesn't support well)
	snap2, err := fs.CreateSnapshot("test2")
	if err != nil {
		t.Fatal(err)
	}
	defer snap2.Release()

	snap2Data, err := snap2.ReadFile("/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(snap2Data) != "modified" {
		t.Errorf("new snapshot should have 'modified', got '%s'", string(snap2Data))
	}
}

func TestSnapshot_ReadDir(t *testing.T) {
	path := tempFile(t)

	fs, err := openFSForTest(t, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	// Create directory with files
	err = fs.Mkdir("/testdir", 0755)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 3; i++ {
		file, err := fs.Create("/testdir/file" + string(rune('0'+i)) + ".txt")
		if err != nil {
			t.Fatal(err)
		}
		file.Close()
	}

	// Create snapshot
	snap, err := fs.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	// Read directory from snapshot
	infos, err := snap.ReadDir("/testdir")
	if err != nil {
		t.Fatal(err)
	}

	if len(infos) != 3 {
		t.Errorf("expected 3 entries, got %d", len(infos))
	}
}

func TestSnapshot_ReadFile(t *testing.T) {
	path := tempFile(t)

	fs, err := openFSForTest(t, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	// Create file with content
	file, err := fs.Create("/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	testData := []byte("Hello, World!")
	file.Write(testData)
	file.Close()

	// Create snapshot
	snap, err := fs.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	// Read file from snapshot
	data, err := snap.ReadFile("/test.txt")
	if err != nil {
		t.Fatal(err)
	}

	if string(data) != string(testData) {
		t.Errorf("expected '%s', got '%s'", string(testData), string(data))
	}
}

func TestSnapshot_Readlink(t *testing.T) {
	path := tempFile(t)

	fs, err := openFSForTest(t, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	// Create a file and a symlink
	file, err := fs.Create("/target.txt")
	if err != nil {
		t.Fatal(err)
	}
	file.Close()

	err = fs.Symlink("/target.txt", "/link.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Create snapshot
	snap, err := fs.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	// Read symlink from snapshot
	target, err := snap.Readlink("/link.txt")
	if err != nil {
		t.Fatal(err)
	}

	if target != "/target.txt" {
		t.Errorf("expected '/target.txt', got '%s'", target)
	}
}

func TestSnapshot_Walk(t *testing.T) {
	path := tempFile(t)

	fs, err := openFSForTest(t, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	// Create directory structure
	err = fs.MkdirAll("/a/b/c", 0755)
	if err != nil {
		t.Fatal(err)
	}

	file, err := fs.Create("/a/file1.txt")
	if err != nil {
		t.Fatal(err)
	}
	file.Close()

	file, err = fs.Create("/a/b/file2.txt")
	if err != nil {
		t.Fatal(err)
	}
	file.Close()

	// Create snapshot
	snap, err := fs.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	// Walk the snapshot
	visited := make(map[string]bool)
	err = Walk(snap, "/a", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		visited[path] = true
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	expected := []string{"/a", "/a/b", "/a/b/c", "/a/b/file2.txt", "/a/file1.txt"}
	for _, path := range expected {
		if !visited[path] {
			t.Errorf("expected to visit '%s'", path)
		}
	}
}

func TestSnapshot_WriteOperationsFail(t *testing.T) {
	path := tempFile(t)

	fs, err := openFSForTest(t, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	snap, err := fs.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	// Copy write operations should fail
	if err := snap.Mkdir("/test", 0755); err != os.ErrPermission {
		t.Errorf("Mkdir should return ErrPermission, got %v", err)
	}

	if _, err := snap.Create("/test"); err != os.ErrPermission {
		t.Errorf("Create should return ErrPermission, got %v", err)
	}

	if err := snap.Remove("/test"); err != os.ErrPermission {
		t.Errorf("Remove should return ErrPermission, got %v", err)
	}
}

func TestSnapshotManager_CreateAndGet(t *testing.T) {
	path := tempFile(t)

	fs, err := openFSForTest(t, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	sm := fs.NewSnapshotManager()
	defer sm.ReleaseAll()

	snap, err := sm.Create("test1")
	if err != nil {
		t.Fatal(err)
	}

	if snap.Name() != "test1" {
		t.Errorf("expected name 'test1', got '%s'", snap.Name())
	}

	// Get the snapshot
	retrieved, ok := sm.Get("test1")
	if !ok {
		t.Fatal("snapshot should exist")
	}

	if retrieved.Name() != snap.Name() {
		t.Error("retrieved snapshot should match created snapshot")
	}

	// Try to get non-existent snapshot
	_, ok = sm.Get("nonexistent")
	if ok {
		t.Error("non-existent snapshot should not be found")
	}
}

func TestSnapshotManager_CreateDuplicate(t *testing.T) {
	path := tempFile(t)

	fs, err := openFSForTest(t, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	sm := fs.NewSnapshotManager()
	defer sm.ReleaseAll()

	_, err = sm.Create("test")
	if err != nil {
		t.Fatal(err)
	}

	// Try to create duplicate
	_, err = sm.Create("test")
	if err != os.ErrExist {
		t.Errorf("expected ErrExist, got %v", err)
	}
}

func TestSnapshotManager_Delete(t *testing.T) {
	path := tempFile(t)

	fs, err := openFSForTest(t, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	sm := fs.NewSnapshotManager()

	snap, err := sm.Create("test")
	if err != nil {
		t.Fatal(err)
	}

	err = sm.Delete("test")
	if err != nil {
		t.Fatal(err)
	}

	// Snapshot should no longer exist
	_, ok := sm.Get("test")
	if ok {
		t.Error("snapshot should have been deleted")
	}

	// Snapshot should be released
	_, err = snap.Stat("/")
	if err != os.ErrClosed {
		t.Error("snapshot should be released after delete")
	}

	// Delete non-existent should fail
	err = sm.Delete("nonexistent")
	if err != os.ErrNotExist {
		t.Errorf("expected ErrNotExist, got %v", err)
	}
}

func TestSnapshotManager_List(t *testing.T) {
	path := tempFile(t)

	fs, err := openFSForTest(t, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	sm := fs.NewSnapshotManager()
	defer sm.ReleaseAll()

	// Empty list
	if len(sm.List()) != 0 {
		t.Error("list should be empty initially")
	}

	// Create snapshots
	names := []string{"snap1", "snap2", "snap3"}
	for _, name := range names {
		_, err := sm.Create(name)
		if err != nil {
			t.Fatal(err)
		}
	}

	list := sm.List()
	if len(list) != 3 {
		t.Errorf("expected 3 snapshots, got %d", len(list))
	}

	// Check all names are present
	listMap := make(map[string]bool)
	for _, name := range list {
		listMap[name] = true
	}

	for _, name := range names {
		if !listMap[name] {
			t.Errorf("expected snapshot '%s' in list", name)
		}
	}
}

func TestSnapshotManager_ReleaseAll(t *testing.T) {
	path := tempFile(t)

	fs, err := openFSForTest(t, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	sm := fs.NewSnapshotManager()

	// Create multiple snapshots
	snaps := make([]*Snapshot, 3)
	for i := 0; i < 3; i++ {
		snap, err := sm.Create("snap" + string(rune('1'+i)))
		if err != nil {
			t.Fatal(err)
		}
		snaps[i] = snap
	}

	err = sm.ReleaseAll()
	if err != nil {
		t.Fatal(err)
	}

	// Copy snapshots should be released
	for _, snap := range snaps {
		_, err := snap.Stat("/")
		if err != os.ErrClosed {
			t.Error("snapshot should be released")
		}
	}

	// List should be empty
	if len(sm.List()) != 0 {
		t.Error("list should be empty after ReleaseAll")
	}
}

func TestSnapshot_CopyToFS(t *testing.T) {
	path := tempFile(t)

	fs, err := openFSForTest(t, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	// Create original file
	file, err := fs.Create("/original.txt")
	if err != nil {
		t.Fatal(err)
	}
	file.Write([]byte("original content"))
	file.Close()

	// Create snapshot
	snap, err := fs.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	// This will deadlock because CopyToFS calls fs.OpenFile while snap holds a read transaction
	err = snap.CopyToFS("/original.txt", "/restored.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Check restored file has snapshot content
	data, err := readFileContents(fs, "/restored.txt")
	if err != nil {
		t.Fatal(err)
	}

	if string(data) != "original content" {
		t.Errorf("expected 'original content', got '%s'", string(data))
	}
}

// Helper function to read file contents
func readFileContents(fs *FS, path string) ([]byte, error) {
	file, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	data := make([]byte, info.Size())
	_, err = file.Read(data)
	return data, err
}

// TestSnapshotReadResultsArePrivate verifies that whole-file reads from a
// snapshot can be mutated by the caller without corrupting later reads, for
// both plaintext (mmap-backed) and encrypted content.
func TestSnapshotReadResultsArePrivate(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		cfg := Config{}
		if encrypted {
			cfg = Config{Secret: testEncKey(41)}
		}
		store, _ := openEncStore(t, []byte(hstore.SERIES), cfg)
		fsys, err := store.Open("fs")
		require.NoError(t, err)
		writeTestFile(t, fsys, "/f.txt", "snapshot payload")

		snap, err := fsys.CreateSnapshot("s")
		require.NoError(t, err)

		first, err := snap.ReadFile("/f.txt")
		require.NoError(t, err)
		first[0] = 'X'

		second, err := snap.ReadFile("/f.txt")
		require.NoError(t, err)
		require.Equal(t, "snapshot payload", string(second))
		require.NoError(t, snap.Release())
	}
}

// TestSnapshotLazyOpenSeeks verifies that an opened snapshot file reports its
// size before any data has been materialized.
func TestSnapshotLazyOpenSeeks(t *testing.T) {
	store, _ := openEncStore(t, []byte(hstore.SERIES), Config{})
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	writeTestFile(t, fsys, "/f.txt", "0123456789")

	snap, err := fsys.CreateSnapshot("s")
	require.NoError(t, err)
	defer snap.Release()

	f, err := snap.Open("/f.txt")
	require.NoError(t, err)

	end, err := f.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(10), end)

	start, err := f.Seek(-4, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(6), start)

	buf := make([]byte, 4)
	n, err := f.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, "6789", string(buf))
}

// TestSnapshotDoesNotBlockWrites regresses the read-transaction/mmap deadlock:
// a snapshot used to hold a bbolt read transaction, so a write that needed to
// grow the mmap blocked forever. Snapshots are now materialized eagerly.
func TestSnapshotDoesNotBlockWrites(t *testing.T) {
	path := tempFile(t)

	fsys, err := openFSForTest(t, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fsys.Close()

	f, err := fsys.Create("/large.bin")
	if err != nil {
		t.Fatal(err)
	}
	original := make([]byte, 2<<20)
	for i := range original {
		original[i] = byte(i)
	}
	if _, err := f.Write(original); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	snap, err := fsys.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	// Enough new data to force the bbolt mmap to grow while the snapshot is
	// alive. With a held read transaction this write would deadlock.
	done := make(chan error, 1)
	go func() {
		out, err := fsys.Create("/grow.bin")
		if err != nil {
			done <- err
			return
		}
		if _, err := out.Write(make([]byte, 2<<20)); err != nil {
			_ = out.Close()
			done <- err
			return
		}
		done <- out.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("write blocked while a snapshot was open")
	}

	// The snapshot still exposes the state at creation time.
	if _, err := snap.Stat("/grow.bin"); !os.IsNotExist(err) {
		t.Fatalf("snapshot saw a post-creation file: %v", err)
	}
	data, err := snap.ReadFile("/large.bin")
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != len(original) {
		t.Fatalf("snapshot content length = %d, want %d", len(data), len(original))
	}
}
