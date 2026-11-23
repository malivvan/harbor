package fstree

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func testFSKey() []byte {
	key := make([]byte, KeySize)
	for i := range key {
		key[i] = byte(i + 1)
	}
	return key
}

// openEncryptedFSForTest opens a filesystem whose content is encrypted in the
// bbolt data bucket.
func openEncryptedFSForTest(t *testing.T) *FS {
	t.Helper()

	db, err := bolt.Open(filepath.Join(t.TempDir(), "enc.db"), 0644, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	store, err := Open(context.Background(), db, Config{Secret: testFSKey(), EncryptContent: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	filesystem, err := store.Open("")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	return filesystem
}

func openEncryptedFSWithContentForTest(t *testing.T, contentFS ContentFS) *FS {
	t.Helper()

	db, err := bolt.Open(filepath.Join(t.TempDir(), "enc-content.db"), 0644, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	store, err := Open(context.Background(), db, Config{Secret: testFSKey(), EncryptContent: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	filesystem, err := store.OpenWithContentFS("", contentFS)
	if err != nil {
		t.Fatalf("OpenWithContentFS: %v", err)
	}
	return filesystem
}

// TestRenameDirectoryIntoOwnSubtree guards F1: the subtree must stay reachable.
func TestRenameDirectoryIntoOwnSubtree(t *testing.T) {
	fsys, err := openFSForTest(t, filepath.Join(t.TempDir(), "rename.db"), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.Mkdir("/a", 0755); err != nil {
		t.Fatal(err)
	}
	writeStoreFile(t, fsys, "/a/f.txt", "keep")

	for _, dst := range []string{"/a/c", "/a/c/d", "/a"} {
		if err := fsys.Rename("/a", dst); err == nil {
			t.Fatalf("Rename(/a, %q) succeeded, want an error", dst)
		}
	}

	data, err := fsys.ReadFile("/a/f.txt")
	if err != nil || string(data) != "keep" {
		t.Fatalf("source was damaged: content %q, err %v", data, err)
	}
}

// TestCopyDirectoryIntoOwnSubtree guards F1 for Copy.
func TestCopyDirectoryIntoOwnSubtree(t *testing.T) {
	fsys, err := openFSForTest(t, filepath.Join(t.TempDir(), "copy.db"), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.Mkdir("/a", 0755); err != nil {
		t.Fatal(err)
	}
	writeStoreFile(t, fsys, "/a/f.txt", "keep")

	if err := fsys.Copy("/a", "/a/c"); err == nil {
		t.Fatal("Copy(/a, /a/c) succeeded, want an error")
	}
	data, err := fsys.ReadFile("/a/f.txt")
	if err != nil || string(data) != "keep" {
		t.Fatalf("source was damaged: content %q, err %v", data, err)
	}
}

// TestEncryptContentWithExternalContentFS guards F2: the external store must
// never hold the plaintext.
func TestEncryptContentWithExternalContentFS(t *testing.T) {
	content := newContentStorageFS(t)
	fsys := openEncryptedFSWithContentForTest(t, content.AsContentFS())

	const secret = "top-secret-payload"
	writeStoreFile(t, fsys, "/secret.txt", secret)

	info, err := fsys.Stat("/secret.txt")
	if err != nil {
		t.Fatal(err)
	}
	ino := info.Sys().(*iNode).Ino
	raw, err := content.ReadFile(inoToPath(ino))
	if err != nil {
		t.Fatalf("read external content: %v", err)
	}
	if bytes.Contains(raw, []byte(secret)) || string(raw) == secret {
		t.Fatalf("external content leaks the plaintext: %q", raw)
	}

	data, err := fsys.ReadFile("/secret.txt")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data) != secret {
		t.Fatalf("content = %q, want %q", data, secret)
	}
}

// TestUmaskAppliedToCreatedEntries guards F4.
func TestUmaskAppliedToCreatedEntries(t *testing.T) {
	fsys, err := openFSForTest(t, filepath.Join(t.TempDir(), "umask.db"), "")
	if err != nil {
		t.Fatal(err)
	}
	fsys.SetUmask(0077)

	f, err := fsys.Create("/private.txt")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := fsys.Stat("/private.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0700 {
		t.Fatalf("file perm = %o, want 0700", got)
	}

	if err := fsys.Mkdir("/private", 0777); err != nil {
		t.Fatal(err)
	}
	info, err = fsys.Stat("/private")
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0700 {
		t.Fatalf("dir perm = %o, want 0700", got)
	}
}

// TestRemoveRootIsBusy guards F15.
func TestRemoveRootIsBusy(t *testing.T) {
	fsys, err := openFSForTest(t, filepath.Join(t.TempDir(), "root.db"), "")
	if err != nil {
		t.Fatal(err)
	}
	err = fsys.Remove("/")
	if !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("Remove(/) = %v, want EBUSY", err)
	}
}

// TestRenameReplacesExistingFile guards F9.
func TestRenameReplacesExistingFile(t *testing.T) {
	fsys, err := openFSForTest(t, filepath.Join(t.TempDir(), "replace.db"), "")
	if err != nil {
		t.Fatal(err)
	}
	writeStoreFile(t, fsys, "/src.txt", "source")
	writeStoreFile(t, fsys, "/dst.txt", "destination")

	if err := fsys.Rename("/src.txt", "/dst.txt"); err != nil {
		t.Fatalf("Rename over existing file: %v", err)
	}
	data, err := fsys.ReadFile("/dst.txt")
	if err != nil || string(data) != "source" {
		t.Fatalf("dst content = %q, %v; want source", data, err)
	}
	if _, err := fsys.Stat("/src.txt"); !os.IsNotExist(err) {
		t.Fatalf("source still exists after rename: %v", err)
	}
}

// TestRenameAcrossDirectoriesIsConsistent guards F10.
func TestRenameAcrossDirectoriesIsConsistent(t *testing.T) {
	fsys, err := openFSForTest(t, filepath.Join(t.TempDir(), "cross.db"), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.MkdirAll("/d1", 0755); err != nil {
		t.Fatal(err)
	}
	if err := fsys.MkdirAll("/d2", 0755); err != nil {
		t.Fatal(err)
	}
	writeStoreFile(t, fsys, "/d1/f.txt", "moved")

	if err := fsys.Rename("/d1/f.txt", "/d2/f.txt"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if _, err := fsys.Stat("/d1/f.txt"); !os.IsNotExist(err) {
		t.Fatalf("old name still resolves: %v", err)
	}
	data, err := fsys.ReadFile("/d2/f.txt")
	if err != nil || string(data) != "moved" {
		t.Fatalf("new name content = %q, %v", data, err)
	}
}

// TestSnapshotSurvivesReadError guards F12: one corrupt file must not end the
// snapshot's shared transaction.
func TestSnapshotSurvivesReadError(t *testing.T) {
	fsys := openEncryptedFSForTest(t)

	writeStoreFile(t, fsys, "/good.txt", "good")
	writeStoreFile(t, fsys, "/bad.txt", "bad")

	info, err := fsys.Stat("/bad.txt")
	if err != nil {
		t.Fatal(err)
	}
	ino := info.Sys().(*iNode).Ino
	if err := fsys.db.Update(func(tx *bolt.Tx) error {
		b, err := fsys.openFsBucket(tx)
		if err != nil {
			return err
		}
		raw := b.data.Get(i2b(ino))
		if len(raw) == 0 {
			return errors.New("no data for inode")
		}
		corrupt := append([]byte(nil), raw...)
		corrupt[len(corrupt)-1] ^= 0xFF
		return b.data.Put(i2b(ino), corrupt)
	}); err != nil {
		t.Fatalf("corrupt data: %v", err)
	}

	snap, err := fsys.CreateSnapshot("s")
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	defer snap.Release()

	if _, err := snap.ReadFile("/bad.txt"); err == nil {
		t.Fatal("corrupt file was readable")
	}
	data, err := snap.ReadFile("/good.txt")
	if err != nil {
		t.Fatalf("good file unreadable after a bad read: %v", err)
	}
	if string(data) != "good" {
		t.Fatalf("good file content = %q", data)
	}

	// Release must not double-rollback or panic.
	if err := snap.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

// TestRootPathValidation guards F19.
func TestRootPathValidation(t *testing.T) {
	db, err := bolt.Open(filepath.Join(t.TempDir(), "root.db"), 0644, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	for _, root := range [][][]byte{
		{[]byte("a/b")},
		{[]byte("..")},
		{[]byte(".")},
	} {
		if _, err := Open(context.Background(), db, Config{Root: root}); err == nil {
			t.Fatalf("Open with root %q succeeded, want an error", root)
		}
	}

	store, err := Open(context.Background(), db, Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a/b", "..", "."} {
		if _, err := store.Open(name); err == nil {
			t.Fatalf("store.Open(%q) succeeded, want an error", name)
		}
	}
}

// TestWalkAndReadmeSignatures ensures the documented helpers still compose.
func TestWalkAndReadmeSignatures(t *testing.T) {
	fsys, err := openFSForTest(t, filepath.Join(t.TempDir(), "walk.db"), "")
	if err != nil {
		t.Fatal(err)
	}
	writeStoreFile(t, fsys, "/a.txt", "x")

	var names []string
	if err := Walk(fsys, "/", func(p string, _ os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		names = append(names, path.Clean(p))
		return nil
	}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if !strings.Contains(strings.Join(names, ","), "/a.txt") {
		t.Fatalf("Walk did not visit the file: %v", names)
	}
}
