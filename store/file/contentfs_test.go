package file

import (
	"context"
	"os"
	"path"
	"sync"
	"testing"
)

// memContentFS implements ContentFS using only standard library types. It
// stands in for an application-defined object store in another module, which
// cannot import the module-internal absfs package.
type memContentFS struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemContentFS() *memContentFS {
	return &memContentFS{data: make(map[string][]byte)}
}

func (m *memContentFS) ReadFile(name string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, ok := m.data[path.Clean(name)]
	if !ok {
		return nil, &os.PathError{Op: "read", Path: name, Err: os.ErrNotExist}
	}
	return append([]byte(nil), raw...), nil
}

func (m *memContentFS) WriteFile(name string, data []byte, _ os.FileMode) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[path.Clean(name)] = append([]byte(nil), data...)
	return nil
}

func (m *memContentFS) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := path.Clean(name)
	if _, ok := m.data[key]; !ok {
		return &os.PathError{Op: "remove", Path: name, Err: os.ErrNotExist}
	}
	delete(m.data, key)
	return nil
}

func (m *memContentFS) has(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.data[path.Clean(name)]
	return ok
}

// drop removes an object outside the FS's knowledge, simulating a lost object.
func (m *memContentFS) drop(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, path.Clean(name))
}

// TestContentFSExportedInterface drives an external content backend that
// implements the exported ContentFS interface with standard types only, so the
// absfs bridge is not required.
func TestContentFSExportedInterface(t *testing.T) {
	content := newMemContentFS()
	store, err := Open(context.Background(), openStoreTestDB(t), Config{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	fsys, err := store.OpenWithContentFS("docs", content)
	if err != nil {
		t.Fatalf("OpenWithContentFS: %v", err)
	}

	writeStoreFile(t, fsys, "/hello.txt", "hello world")

	info, err := fsys.Stat("/hello.txt")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	ino := info.Sys().(*iNode).Ino
	if info.Size() != int64(len("hello world")) {
		t.Fatalf("size = %d, want %d", info.Size(), len("hello world"))
	}
	if !content.has(inoToPath(ino)) {
		t.Fatal("content was not written to the external backend")
	}

	// Reads must come from the external backend.
	f, err := fsys.Open("/hello.txt")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	buf := make([]byte, len("hello world"))
	if _, err := f.Read(buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if string(buf) != "hello world" {
		t.Fatalf("read %q, want %q", buf, "hello world")
	}

	// Truncating to zero removes the object and reads as an empty file.
	if err := fsys.Truncate("/hello.txt", 0); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if content.has(inoToPath(ino)) {
		t.Fatal("truncate to zero left the content object behind")
	}
	data, err := fsys.loadData(ino)
	if err != nil {
		t.Fatalf("loadData after truncate: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("loadData after truncate = %q, want empty", data)
	}

	// Removing the file deletes its object.
	writeStoreFile(t, fsys, "/bye.txt", "bye")
	info, err = fsys.Stat("/bye.txt")
	if err != nil {
		t.Fatalf("stat bye: %v", err)
	}
	byeIno := info.Sys().(*iNode).Ino
	if !content.has(inoToPath(byeIno)) {
		t.Fatal("content was not written to the external backend")
	}
	if err := fsys.Remove("/bye.txt"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if content.has(inoToPath(byeIno)) {
		t.Fatal("remove left the content object behind")
	}
}

// TestContentFSMissingObjectReadsEmpty pins the os.ErrNotExist contract: a
// missing object must be treated as an empty file, not as an error.
func TestContentFSMissingObjectReadsEmpty(t *testing.T) {
	content := newMemContentFS()
	store, err := Open(context.Background(), openStoreTestDB(t), Config{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	fsys, err := store.OpenWithContentFS("docs", content)
	if err != nil {
		t.Fatalf("OpenWithContentFS: %v", err)
	}

	writeStoreFile(t, fsys, "/empty.txt", "")
	info, err := fsys.Stat("/empty.txt")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	ino := info.Sys().(*iNode).Ino

	// Simulate an object that was never written (or was lost).
	content.drop(inoToPath(ino))

	f, err := fsys.Open("/empty.txt")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Read(make([]byte, 1)); err == nil {
		t.Fatal("reading an empty file must report EOF")
	}
}
