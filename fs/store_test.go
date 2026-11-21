package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

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
