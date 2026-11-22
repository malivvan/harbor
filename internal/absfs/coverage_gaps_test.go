package absfs

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newCoverFiler() *mockFiler {
	m := newMockFiler()
	now := time.Now()
	m.files["/"] = &mockFile{name: "/", mode: os.ModeDir | 0755, isDir: true, modTime: now}
	m.files["/dir"] = &mockFile{name: "/dir", mode: os.ModeDir | 0755, isDir: true, modTime: now}
	m.files["/dir/sub"] = &mockFile{name: "/dir/sub", mode: os.ModeDir | 0755, isDir: true, modTime: now}
	m.files["/dir/a.txt"] = &mockFile{name: "/dir/a.txt", mode: 0644, content: []byte("abc"), modTime: now}
	m.files["/dir/sub/b.txt"] = &mockFile{name: "/dir/sub/b.txt", mode: 0644, content: []byte("B"), modTime: now}
	m.files["/target"] = &mockFile{name: "/target", mode: 0644, content: []byte("target"), modTime: now}
	m.files["/dir"].entries = []os.FileInfo{m.files["/dir/a.txt"], m.files["/dir/sub"]}
	m.files["/dir/sub"].entries = []os.FileInfo{m.files["/dir/sub/b.txt"]}
	return m
}

// finiteDirFiler ends a directory listing after one call, which is what
// removeAll expects from a real file handle.
type finiteDirFiler struct {
	*mockFiler
}

func (f *finiteDirFiler) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	file, err := f.mockFiler.OpenFile(name, flag, perm)
	if err != nil {
		return file, err
	}
	return &finiteDirHandle{File: file}, nil
}

type finiteDirHandle struct {
	File
	drained bool
}

func (h *finiteDirHandle) Readdirnames(n int) ([]string, error) {
	if h.drained {
		return nil, io.EOF
	}
	names, err := h.File.Readdirnames(n)
	if err == nil {
		h.drained = true
	}
	return names, err
}

func TestExtendedFSRemoveAllFallback(t *testing.T) {
	m := newCoverFiler()
	efs := ExtendFiler(&finiteDirFiler{m}).(*extendedFS)

	require.NoError(t, efs.RemoveAll("/dir"))
	_, exists := m.files["/dir"]
	require.False(t, exists)
	_, exists = m.files["/dir/sub/b.txt"]
	require.False(t, exists)

	// Missing paths are not an error.
	require.NoError(t, efs.RemoveAll("/missing"))

	// A plain file is removed directly.
	require.NoError(t, efs.RemoveAll("/target"))
	_, exists = m.files["/target"]
	require.False(t, exists)
}

func TestExtendedFSTruncateFallback(t *testing.T) {
	m := newCoverFiler()
	efs := ExtendFiler(m).(*extendedFS)

	require.NoError(t, efs.Truncate("/dir/a.txt", 2))
	require.Equal(t, "ab", string(m.files["/dir/a.txt"].content))

	require.NoError(t, efs.Truncate("/dir/a.txt", 5))
	require.Equal(t, []byte{'a', 'b', 0, 0, 0}, m.files["/dir/a.txt"].content)

	require.ErrorIs(t, efs.Truncate("/dir/a.txt", -1), os.ErrInvalid)
	require.ErrorIs(t, efs.Truncate("/dir/a.txt", maxTruncateSize+1), os.ErrInvalid)
	require.Error(t, efs.Truncate("/missing", 1))
}

func TestExtendedFSAccessors(t *testing.T) {
	m := newCoverFiler()
	efs := ExtendFiler(m).(*extendedFS)

	entries, err := efs.ReadDir("/dir")
	require.NoError(t, err)
	require.Len(t, entries, 2)

	data, err := efs.ReadFile("/dir/a.txt")
	require.NoError(t, err)
	require.Equal(t, "abc", string(data))

	sub, err := efs.Sub("/dir")
	require.NoError(t, err)
	require.Equal(t, "abc", string(mustFSRead(t, sub, "a.txt")))

	// A filer without native symlink support reports unsupported operations,
	// while Lstat falls back to Stat.
	info, err := efs.Lstat("/dir/a.txt")
	require.NoError(t, err)
	require.Equal(t, "a.txt", info.Name())
	require.Error(t, efs.Lchown("/dir/a.txt", 0, 0))
	_, err = efs.Readlink("/dir/a.txt")
	require.Error(t, err)
	require.Error(t, efs.Symlink("/dir/a.txt", "/link"))

	require.NotEmpty(t, efs.TempDir())
	_, err = efs.Getwd()
	require.NoError(t, err)
	require.NoError(t, efs.Chdir("/"))
	require.NoError(t, efs.Chdir("/dir"))
}

func mustFSRead(t *testing.T, f fs.FS, name string) []byte {
	t.Helper()
	data, err := fs.ReadFile(f, name)
	require.NoError(t, err)
	return data
}

func TestFilerToFSValidation(t *testing.T) {
	m := newCoverFiler()

	_, err := FilerToFS(m, "/missing")
	require.Error(t, err)
	_, err = FilerToFS(m, "/dir/a.txt")
	require.Error(t, err)

	subAny, err := FilerToFS(m, "/dir")
	require.NoError(t, err)
	sub := subAny.(*subFS)
	require.Equal(t, "abc", string(mustFSRead(t, sub, "a.txt")))

	// fs.FS path validation is enforced.
	_, err = sub.Open("../a.txt")
	require.ErrorIs(t, err, fs.ErrInvalid)
	_, err = sub.ReadDir("/absolute")
	require.ErrorIs(t, err, fs.ErrInvalid)
	_, err = sub.ReadFile("../a.txt")
	require.ErrorIs(t, err, fs.ErrInvalid)
	_, err = sub.Stat("")
	require.ErrorIs(t, err, fs.ErrInvalid)
	_, err = sub.Sub("../x")
	require.ErrorIs(t, err, fs.ErrInvalid)

	sf := fs.SubFS(sub)
	nested, err := sf.Sub("sub")
	require.NoError(t, err)
	require.Equal(t, "B", string(mustFSRead(t, nested, "b.txt")))

	_, err = sub.Open("missing")
	require.Error(t, err)
	_, err = sub.ReadDir("missing")
	require.Error(t, err)
	_, err = sub.ReadFile("missing")
	require.Error(t, err)
	_, err = sub.Stat("missing")
	require.Error(t, err)
	_, err = sf.Sub("missing")
	require.Error(t, err)

	// A root of "." exercises the no-prefix branch of subFS.
	dot := &subFS{filer: m, root: "."}
	_, err = dot.Open("x")
	require.Error(t, err) // the mock has no "x"

	// Cover the empty-root branch too.
	empty := &subFS{filer: m, root: ""}
	_, err = empty.Open("x")
	require.Error(t, err)
}

func TestFileAdapterReadDirFallback(t *testing.T) {
	info := &mockFile{name: "x", mode: 0644, modTime: time.Now()}
	dir := &testSeekable{name: "dir", isDir: true, entries: []os.FileInfo{info}}
	adapter := ExtendSeekable(dir)

	entries, err := adapter.ReadDir(-1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "x", entries[0].Name())

	file := &testSeekable{name: "file", data: []byte("x")}
	_, err = ExtendSeekable(file).ReadDir(-1)
	require.Error(t, err)
}

func TestSymlinkOverlayLifecycle(t *testing.T) {
	m := newCoverFiler()
	base := ExtendFiler(&finiteDirFiler{m})
	o := NewSymlinkOverlay(base)
	require.Same(t, base, o.Base())

	require.NoError(t, o.Symlink("/target", "/link"))
	info, err := o.Lstat("/link")
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&os.ModeSymlink)

	target, err := o.Readlink("/link")
	require.NoError(t, err)
	require.Equal(t, "/target", target)

	data, err := o.ReadFile("/link")
	require.NoError(t, err)
	require.Equal(t, "target", string(data))

	_, err = o.Stat("/link")
	require.NoError(t, err)
	require.NoError(t, o.Chmod("/link", 0600))
	require.NoError(t, o.Chtimes("/link", time.Now(), time.Now()))
	require.NoError(t, o.Chown("/link", 0, 0))
	require.NoError(t, o.Lchown("/link", 0, 0))
	require.NoError(t, o.Truncate("/link", 2))
	require.Equal(t, "ta", string(m.files["/target"].content))

	// Relative targets resolve against the link's directory.
	require.NoError(t, o.Symlink("target", "/rel"))
	data, err = o.ReadFile("/rel")
	require.NoError(t, err)
	require.Equal(t, "ta", string(data))

	// ".." components are normalised by resolvePath.
	_, err = o.Stat("/dir/../target")
	require.NoError(t, err)

	// Creating a link whose parent is missing creates the parent.
	require.NoError(t, o.Symlink("/target", "/nested/deep/link"))
	_, err = o.Lstat("/nested/deep/link")
	require.NoError(t, err)

	// Creating an existing path is rejected.
	err = o.Symlink("/target", "/link")
	require.True(t, errors.Is(err, syscall.EEXIST))

	require.NoError(t, o.Mkdir("/newdir", 0755))
	_, err = o.Create("/newfile")
	require.NoError(t, err)
	require.NoError(t, o.MkdirAll("/a/b", 0755))
	_, err = o.Open("/target")
	require.NoError(t, err)
	_, err = o.ReadDir("/dir")
	require.NoError(t, err)
	_, err = o.Sub("/dir")
	require.NoError(t, err)
	require.NoError(t, o.Chdir("/dir"))
	_, err = o.Getwd()
	require.NoError(t, err)
	require.NotEmpty(t, o.TempDir())
	require.NoError(t, o.RemoveAll("/a"))
	require.NoError(t, o.Remove("/link"))

	// Broken and cyclic links.
	require.NoError(t, o.Symlink("/missing", "/broken"))
	_, err = o.ReadFile("/broken")
	require.Error(t, err)

	require.NoError(t, o.Symlink("/c2", "/c1"))
	require.NoError(t, o.Symlink("/c1", "/c2"))
	_, err = o.ReadFile("/c1")
	require.Error(t, err)

	// Readlink rejects non-links and malformed overlay entries.
	_, err = o.Readlink("/target")
	require.Error(t, err)
	require.Zero(t, m.files["/target"].mode&os.ModeSymlink)
	m.files["/fake"] = &mockFile{name: "/fake", mode: os.ModeSymlink | 0777, content: []byte("not-magic")}
	_, err = o.Readlink("/fake")
	require.Error(t, err)
	_, err = o.Readlink("/missing")
	require.Error(t, err)
}

func TestNoSymlinks(t *testing.T) {
	base := ExtendFiler(newCoverFiler())
	n := BlockSymlinks(base)
	require.NotNil(t, n)

	err := n.Symlink("/a", "/b")
	require.True(t, errors.Is(err, syscall.ENOTSUP))
	_, err = n.Readlink("/a")
	require.True(t, errors.Is(err, syscall.ENOTSUP))

	// Lstat/Lchown fall through to the base filesystem.
	info, err := n.Lstat("/dir/a.txt")
	require.NoError(t, err)
	require.Equal(t, "a.txt", info.Name())
	require.NoError(t, n.Lchown("/dir/a.txt", 0, 0))
}

func TestExtendedFSMkdirAll(t *testing.T) {
	m := newMockFiler()
	m.files["/"] = &mockFile{name: "/", mode: os.ModeDir | 0755, isDir: true}
	efs := ExtendFiler(m).(*extendedFS)

	require.NoError(t, efs.MkdirAll("/a/b/c", 0755))
	require.True(t, m.files["/a"].isDir)
	require.True(t, m.files["/a/b"].isDir)
	require.True(t, m.files["/a/b/c"].isDir)

	// An existing directory is accepted, an existing file is ENOTDIR.
	require.NoError(t, efs.MkdirAll("/a/b", 0755))
	m.files["/a/file"] = &mockFile{name: "/a/file", mode: 0644}
	err := efs.MkdirAll("/a/file/b", 0755)
	require.True(t, errors.Is(err, syscall.ENOTDIR))
}

func TestPathNormalizationHelpers(t *testing.T) {
	require.Equal(t, "/a/b", toSlash(`\a\b`))
	require.True(t, isVirtualAbs("/a"))
	require.False(t, isVirtualAbs("a"))
	require.Equal(t, "/a/b", path.Clean("/a/./b/../b"))
}
