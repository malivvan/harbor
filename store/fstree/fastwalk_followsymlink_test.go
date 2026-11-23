package fstree

import (
	iofs "io/fs"
	"os"
	"path"
	"sort"
	"testing"
	"time"

	"github.com/malivvan/harbor/internal/absfs"
)

// fsTestInfo is a minimal os.FileInfo for the follow-symlink walker test.
type fsTestInfo struct {
	name string
	mode iofs.FileMode
}

func (i fsTestInfo) Name() string        { return i.name }
func (i fsTestInfo) Size() int64         { return 0 }
func (i fsTestInfo) Mode() iofs.FileMode { return i.mode }
func (i fsTestInfo) ModTime() time.Time  { return time.Time{} }
func (i fsTestInfo) IsDir() bool         { return i.mode.IsDir() }
func (i fsTestInfo) Sys() interface{}    { return nil }

// fsTestEntry is a minimal iofs.DirEntry for the follow-symlink walker test.
type fsTestEntry struct {
	name string
	mode iofs.FileMode
}

func (e fsTestEntry) Name() string        { return e.name }
func (e fsTestEntry) IsDir() bool         { return e.mode.IsDir() }
func (e fsTestEntry) Type() iofs.FileMode { return e.mode.Type() }
func (e fsTestEntry) Info() (iofs.FileInfo, error) {
	return fsTestInfo(e), nil
}

// linkWalkFiler is an in-memory absfs.Filer where "/link" is a symlink whose
// target "/dir" is readable via ReadDir("/link") as well as ReadDir("/dir").
type linkWalkFiler struct {
	trees map[string][]iofs.DirEntry
}

func (f *linkWalkFiler) Stat(name string) (os.FileInfo, error) {
	switch path.Clean(name) {
	case "/", "/dir", "/link":
		return fsTestInfo{path.Base(path.Clean(name)), iofs.ModeDir | 0755}, nil
	case "/dir/file.txt", "/link/file.txt":
		return fsTestInfo{"file.txt", 0644}, nil
	}
	return nil, os.ErrNotExist
}

func (f *linkWalkFiler) ReadDir(name string) ([]iofs.DirEntry, error) {
	ents, ok := f.trees[path.Clean(name)]
	if !ok {
		return nil, os.ErrNotExist
	}
	out := make([]iofs.DirEntry, len(ents))
	copy(out, ents)
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out, nil
}

func (f *linkWalkFiler) OpenFile(string, int, os.FileMode) (absfs.File, error) {
	return nil, os.ErrInvalid
}
func (f *linkWalkFiler) Mkdir(string, os.FileMode) error            { return os.ErrInvalid }
func (f *linkWalkFiler) Remove(string) error                        { return os.ErrInvalid }
func (f *linkWalkFiler) Rename(string, string) error                { return os.ErrInvalid }
func (f *linkWalkFiler) Chmod(string, os.FileMode) error            { return os.ErrInvalid }
func (f *linkWalkFiler) Chtimes(string, time.Time, time.Time) error { return os.ErrInvalid }
func (f *linkWalkFiler) Chown(string, int, int) error               { return os.ErrInvalid }
func (f *linkWalkFiler) ReadFile(string) ([]byte, error)            { return nil, os.ErrInvalid }
func (f *linkWalkFiler) Sub(string) (iofs.FS, error)                { return nil, os.ErrInvalid }

func newLinkWalkFiler() *linkWalkFiler {
	return &linkWalkFiler{trees: map[string][]iofs.DirEntry{
		"/": {
			fsTestEntry{"dir", iofs.ModeDir | 0755},
			fsTestEntry{"link", iofs.ModeSymlink},
		},
		"/dir":  {fsTestEntry{"file.txt", 0644}},
		"/link": {fsTestEntry{"file.txt", 0644}},
	}}
}

// TestFastWalkFollowSymlinks verifies that FollowSymlinks=true traverses
// symlinked directories while the default leaves them unvisited.
func TestFastWalkFollowSymlinks(t *testing.T) {
	collect := func(follow bool) map[string]bool {
		filer := newLinkWalkFiler()
		visited := map[string]bool{}
		config := &FastWalkConfig{NumWorkers: 1, FollowSymlinks: follow}
		err := FastWalkWithConfig(filer, "/", config, func(p string, d iofs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			visited[p] = true
			return nil
		})
		if err != nil {
			t.Fatalf("FastWalk(follow=%v): %v", follow, err)
		}
		return visited
	}

	withFollow := collect(true)
	if !withFollow["/link/file.txt"] {
		t.Errorf("FollowSymlinks=true should traverse /link, visited=%v", keys(withFollow))
	}

	without := collect(false)
	if without["/link/file.txt"] {
		t.Errorf("FollowSymlinks=false should not traverse /link, visited=%v", keys(without))
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
