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

func TestSubFilerWritableWrapper(t *testing.T) {
	m := newCoverFiler()
	efs := ExtendFiler(m).(*extendedFS)
	sf := &subFiler{efs: efs, dir: "/dir"}

	require.Equal(t, "/dir/a.txt", sf.fullPath("a.txt"))
	require.Equal(t, "/dir/a.txt", sf.fullPath("../a.txt"))

	f, err := sf.OpenFile("a.txt", os.O_RDONLY, 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, sf.Mkdir("newdir", 0755))
	require.True(t, m.files["/dir/newdir"].isDir)
	require.NoError(t, sf.Rename("a.txt", "b.txt"))
	require.Error(t, sf.Rename("missing", "x"))
	_, err = sf.Stat("b.txt")
	require.NoError(t, err)
	require.NoError(t, sf.Chmod("b.txt", 0600))
	require.NoError(t, sf.Chtimes("b.txt", time.Now(), time.Now()))
	require.NoError(t, sf.Chown("b.txt", 0, 0))
	_, err = sf.ReadDir(".")
	require.NoError(t, err)
	_, err = sf.ReadFile("b.txt")
	require.NoError(t, err)
	require.NoError(t, sf.Remove("b.txt"))

	sub, err := sf.Sub("sub")
	require.NoError(t, err)
	require.Equal(t, "B", string(mustFSRead(t, sub, "b.txt")))
}

// symlinkOnlyFiler is a Filer with SymLinker but no FileSystem methods.
type symlinkOnlyFiler struct{ *mockFiler }

func (s *symlinkOnlyFiler) Lstat(name string) (os.FileInfo, error) { return s.Stat(name) }
func (s *symlinkOnlyFiler) Lchown(name string, uid, gid int) error { return s.Chown(name, uid, gid) }
func (s *symlinkOnlyFiler) Readlink(name string) (string, error)   { return "target", nil }
func (s *symlinkOnlyFiler) Symlink(oldname, newname string) error  { return nil }

// combinedFiler is FileSystem plus SymLinker but not SymlinkFileSystem.
type combinedFiler struct {
	FileSystem
}

func (c *combinedFiler) Lstat(name string) (os.FileInfo, error) { return c.Stat(name) }
func (c *combinedFiler) Lchown(name string, uid, gid int) error { return c.Chown(name, uid, gid) }
func (c *combinedFiler) Readlink(name string) (string, error)   { return "", nil }
func (c *combinedFiler) Symlink(oldname, newname string) error  { return nil }

func TestExtendSymlinkFilerBranches(t *testing.T) {
	base := ExtendFiler(newCoverFiler())

	// Already a SymlinkFileSystem: returned unchanged.
	overlay := NewSymlinkOverlay(base)
	require.Same(t, overlay, ExtendSymlinkFiler(overlay))

	// FileSystem + SymLinker already satisfies SymlinkFileSystem, so it is
	// returned as-is.
	combined := &combinedFiler{FileSystem: base}
	require.Same(t, combined, ExtendSymlinkFiler(combined))

	// SymLinker without FileSystem: the filer is extended and combined.
	linkOnly := ExtendSymlinkFiler(&symlinkOnlyFiler{newCoverFiler()})
	require.IsType(t, &symlinkFS{}, linkOnly)
	_, err := linkOnly.Lstat("/dir/a.txt")
	require.NoError(t, err)

	// No symlink support: an overlay is added.
	plain := ExtendSymlinkFiler(newCoverFiler())
	require.IsType(t, &SymlinkOverlay{}, plain)
	require.NoError(t, plain.Symlink("/target", "/link2"))
}

func TestInvalidFileReadDir(t *testing.T) {
	f := &InvalidFile{Path: "bad"}
	_, err := f.ReadDir(-1)
	require.True(t, errors.Is(err, syscall.EBADF))
}

func TestOverlayRenameAndRenamedInfo(t *testing.T) {
	m := newCoverFiler()
	o := NewSymlinkOverlay(ExtendFiler(&finiteDirFiler{m}))

	info, err := o.Stat("/target")
	require.NoError(t, err)
	renamed, ok := info.(*renamedFileInfo)
	require.True(t, ok)
	require.Equal(t, "target", renamed.Name())

	require.NoError(t, o.Rename("/target", "/moved"))
	_, err = o.Stat("/moved")
	require.NoError(t, err)
	_, err = o.Stat("/target")
	require.Error(t, err)
}

// failingFiler lets a test fail a specific operation.
type failingFiler struct {
	*mockFiler
	failOpen    map[string]error
	failStat    map[string]error
	failReaddir map[string]error
	failClose   bool
}

func (f *failingFiler) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	if err := f.failOpen[name]; err != nil {
		return nil, err
	}
	file, err := f.mockFiler.OpenFile(name, flag, perm)
	if err != nil {
		return file, err
	}
	return &failingHandle{File: file, name: name, parent: f}, nil
}

type failingHandle struct {
	File
	name    string
	parent  *failingFiler
	drained bool
}

func (h *failingHandle) Stat() (os.FileInfo, error) {
	if err := h.parent.failStat[h.name]; err != nil {
		return nil, err
	}
	return h.File.Stat()
}

func (h *failingHandle) Readdirnames(n int) ([]string, error) {
	if err := h.parent.failReaddir[h.name]; err != nil {
		return nil, err
	}
	if h.drained {
		return nil, io.EOF
	}
	h.drained = true
	return h.File.Readdirnames(n)
}

func (h *failingHandle) Close() error {
	if h.parent.failClose {
		return errors.New("close failed")
	}
	return h.File.Close()
}

func TestExtendedFSRemoveAllErrorBranches(t *testing.T) {
	m := newCoverFiler()
	ff := &failingFiler{
		mockFiler:   m,
		failOpen:    map[string]error{"/denied": os.ErrPermission},
		failStat:    map[string]error{"/badstat": errors.New("stat")},
		failReaddir: map[string]error{"/badread": errors.New("readdir")},
	}
	m.files["/badstat"] = &mockFile{name: "/badstat", mode: os.ModeDir | 0755, isDir: true}
	m.files["/badread"] = &mockFile{name: "/badread", mode: os.ModeDir | 0755, isDir: true}
	m.files["/empty"] = &mockFile{name: "/empty", mode: os.ModeDir | 0755, isDir: true}
	efs := ExtendFiler(ff).(*extendedFS)

	require.ErrorIs(t, efs.RemoveAll("/denied"), os.ErrPermission)
	require.Error(t, efs.RemoveAll("/badstat"))
	require.Error(t, efs.RemoveAll("/badread"))

	// A Close error from the final handle of a directory is surfaced.
	ff.failClose = true
	require.Error(t, efs.RemoveAll("/empty"))
}

func TestExtendedFSSymlinkDelegation(t *testing.T) {
	base := ExtendFiler(newCoverFiler())
	overlay := NewSymlinkOverlay(base)
	require.NoError(t, overlay.Symlink("/target", "/link"))
	efs := ExtendFiler(overlay).(*extendedFS)

	// Delegation branch of each symlink method.
	info, err := efs.Lstat("/link")
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&os.ModeSymlink)
	require.NoError(t, efs.Lchown("/link", 0, 0))
	target, err := efs.Readlink("/link")
	require.NoError(t, err)
	require.Equal(t, "/target", target)
	require.NoError(t, efs.Symlink("/target", "/link2"))
	_, err = efs.Readlink("/link2")
	require.NoError(t, err)
}

// failingPropsFiler injects failures into individual operations.
type failingPropsFiler struct {
	*mockFiler
	failMkdirAll bool
	failRemove   bool
	failWrite    bool
	failClose    bool
}

func (f *failingPropsFiler) MkdirAll(name string, perm os.FileMode) error {
	if f.failMkdirAll {
		return errors.New("mkdirall failed")
	}
	return ExtendFiler(f.mockFiler).MkdirAll(name, perm)
}

func (f *failingPropsFiler) Remove(name string) error {
	if f.failRemove {
		return errors.New("remove failed")
	}
	return f.mockFiler.Remove(name)
}

func (f *failingPropsFiler) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	if f.failWrite && flag&(os.O_WRONLY|os.O_RDWR) != 0 {
		return nil, errors.New("write open failed")
	}
	file, err := f.mockFiler.OpenFile(name, flag, perm)
	if err != nil {
		return file, err
	}
	return &closingHandle{File: file, fail: f.failClose}, nil
}

type closingHandle struct {
	File
	fail bool
}

func (h *closingHandle) Close() error {
	if h.fail {
		return errors.New("close failed")
	}
	return h.File.Close()
}

func TestExtendedFSTruncateErrorPaths(t *testing.T) {
	m := newCoverFiler()
	efs := ExtendFiler(m).(*extendedFS)

	// Read error.
	require.Error(t, efs.Truncate("/missing", 1))

	// Open-for-write error.
	ff := &failingPropsFiler{mockFiler: newCoverFiler(), failWrite: true}
	require.Error(t, ExtendFiler(ff).Truncate("/dir/a.txt", 1))

	// Close error.
	cf := &failingPropsFiler{mockFiler: newCoverFiler(), failClose: true}
	require.Error(t, ExtendFiler(cf).Truncate("/dir/a.txt", 1))
}

func TestExtendedFSRemoveAllRemoveError(t *testing.T) {
	ff := &failingPropsFiler{mockFiler: newCoverFiler(), failRemove: true}
	require.Error(t, ExtendFiler(ff).RemoveAll("/dir/a.txt"))
}

func TestOverlayErrorPaths(t *testing.T) {
	base := ExtendFiler(newCoverFiler())
	o := NewSymlinkOverlay(base)
	require.NoError(t, o.Symlink("/missing", "/broken"))

	// Resolution fails for every operation that follows links.
	_, err := o.OpenFile("/broken", os.O_RDONLY, 0)
	require.Error(t, err)
	require.Error(t, o.Mkdir("/broken", 0755))
	_, err = o.Stat("/broken")
	require.Error(t, err)
	require.Error(t, o.Chmod("/broken", 0600))
	require.Error(t, o.Chtimes("/broken", time.Now(), time.Now()))
	require.Error(t, o.Chown("/broken", 0, 0))
	require.Error(t, o.Truncate("/broken", 1))
	_, err = o.ReadDir("/broken")
	require.Error(t, err)
	_, err = o.ReadFile("/broken")
	require.Error(t, err)
	// Rename and Remove do not follow links.
	require.NoError(t, o.Rename("/broken", "/renamed-broken"))
	require.NoError(t, o.Remove("/renamed-broken"))

	// A missing parent during symlink creation is reported.
	failing := &failingPropsFiler{mockFiler: newCoverFiler(), failMkdirAll: true}
	fo := NewSymlinkOverlay(ExtendFiler(failing))
	require.Error(t, fo.Symlink("/target", "/nested/link"))

	// Relative path resolution with a non-navigating filer.
	efs := ExtendFiler(o).(*extendedFS)
	_, err = efs.Lstat("relative")
	require.Error(t, err)
	require.Error(t, efs.Lchown("relative", 0, 0))
	_, err = efs.Readlink("relative")
	require.Error(t, err)
	require.NoError(t, efs.Symlink("/target", "relative-link"))
}
