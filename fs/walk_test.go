package fs

import (
	"os"
	"path/filepath"
	"testing"
)

func newWalkTestFS(t *testing.T) *FileSystem {
	t.Helper()

	path := tempFile()
	t.Cleanup(func() { _ = os.Remove(path) })

	filesystem, err := openFSForTest(t, path, "")
	if err != nil {
		t.Fatalf("openFSForTest: %v", err)
	}

	for _, dir := range []string{"/F", "/F/B", "/F/B/D", "/F/G", "/F/G/I"} {
		if err := filesystem.Mkdir(dir, 0755); err != nil {
			t.Fatalf("Mkdir(%q): %v", dir, err)
		}
	}
	for _, name := range []string{"/F/B/A", "/F/B/D/C", "/F/B/D/E", "/F/G/I/H"} {
		f, err := filesystem.Create(name)
		if err != nil {
			t.Fatalf("Create(%q): %v", name, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("Close(%q): %v", name, err)
		}
	}

	return filesystem
}

func TestWalk(t *testing.T) {
	filesystem := newWalkTestFS(t)

	var got []string
	err := Walk(filesystem, "/", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info == nil {
			t.Fatalf("nil FileInfo for %q", path)
		}
		got = append(got, path)
		return nil
	})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}

	want := []string{
		"/",
		"/F", "/F/B", "/F/B/A", "/F/B/D", "/F/B/D/C", "/F/B/D/E",
		"/F/G", "/F/G/I", "/F/G/I/H",
	}
	if len(got) != len(want) {
		t.Fatalf("visited %d paths, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("path %d = %q, want %q (full order: %v)", i, got[i], want[i], got)
		}
	}
}

func TestWalkSkipDir(t *testing.T) {
	filesystem := newWalkTestFS(t)

	var got []string
	err := Walk(filesystem, "/", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		got = append(got, path)
		if path == "/F/B" {
			return filepath.SkipDir
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}

	for _, skipped := range []string{"/F/B/A", "/F/B/D", "/F/B/D/C", "/F/B/D/E"} {
		for _, path := range got {
			if path == skipped {
				t.Fatalf("Walk visited %q despite SkipDir: %v", skipped, got)
			}
		}
	}
	if len(got) == 0 || got[len(got)-1] != "/F/G/I/H" {
		t.Fatalf("walk did not continue after SkipDir: %v", got)
	}
}

func TestWalkStopsOnError(t *testing.T) {
	filesystem := newWalkTestFS(t)

	sentinel := os.ErrPermission
	err := Walk(filesystem, "/", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if path == "/F" {
			return sentinel
		}
		return nil
	})
	if err != sentinel {
		t.Fatalf("Walk error = %v, want %v", err, sentinel)
	}
}

func TestWalkMissingRoot(t *testing.T) {
	filesystem := newWalkTestFS(t)

	if err := Walk(filesystem, "/does-not-exist", func(string, os.FileInfo, error) error {
		return nil
	}); !os.IsNotExist(err) {
		t.Fatalf("Walk(missing) = %v, want os.ErrNotExist", err)
	}
}
