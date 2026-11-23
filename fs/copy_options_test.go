package fs

import (
	"context"
	"errors"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func openCopyTestFS(t *testing.T) *FS {
	t.Helper()
	fsys, err := openFSForTest(t, tempFile(t), "")
	if err != nil {
		t.Fatalf("open fs: %v", err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	return fsys
}

func readAll(t *testing.T, fsys *FS, name string) string {
	t.Helper()
	data, err := fsys.ReadFile(name)
	require.NoError(t, err)
	return string(data)
}

func TestCopyOptionsOverwrite(t *testing.T) {
	fsys := openCopyTestFS(t)
	writeStoreFile(t, fsys, "/a.txt", "one")
	writeStoreFile(t, fsys, "/b.txt", "two")

	// The zero options preserve Copy's behavior.
	require.ErrorIs(t, fsys.Copy("/a.txt", "/b.txt"), os.ErrExist)
	require.Equal(t, "two", readAll(t, fsys, "/b.txt"))

	require.NoError(t, fsys.CopyWithOptions("/a.txt", "/b.txt", CopyOptions{Overwrite: true}))
	require.Equal(t, "one", readAll(t, fsys, "/b.txt"))

	// A second overwrite replaces the content again.
	writeStoreFile(t, fsys, "/a.txt", "three")
	require.NoError(t, fsys.CopyWithOptions("/a.txt", "/b.txt", CopyOptions{Overwrite: true}))
	require.Equal(t, "three", readAll(t, fsys, "/b.txt"))
}

func TestCopyOptionsOverwriteDirectories(t *testing.T) {
	fsys := openCopyTestFS(t)
	require.NoError(t, fsys.Mkdir("/src", 0o755))
	writeStoreFile(t, fsys, "/src/file.txt", "new")
	require.NoError(t, fsys.Mkdir("/dst", 0o755))
	writeStoreFile(t, fsys, "/dst/old.txt", "old")

	// A non-empty destination directory needs an explicit RemoveAll first.
	require.ErrorIs(t, fsys.CopyWithOptions("/src", "/dst", CopyOptions{Overwrite: true}), syscall.ENOTEMPTY)
	require.Equal(t, "old", readAll(t, fsys, "/dst/old.txt"))

	// An empty destination directory can be replaced.
	require.NoError(t, fsys.Mkdir("/empty", 0o755))
	require.NoError(t, fsys.CopyWithOptions("/src", "/empty", CopyOptions{Overwrite: true}))
	require.Equal(t, "new", readAll(t, fsys, "/empty/file.txt"))

	// A file cannot replace a directory, and a directory cannot replace a file.
	require.ErrorIs(t, fsys.CopyWithOptions("/src/file.txt", "/dst", CopyOptions{Overwrite: true}), syscall.EISDIR)
	require.ErrorIs(t, fsys.CopyWithOptions("/src", "/dst/old.txt", CopyOptions{Overwrite: true}), syscall.EEXIST)
}

func TestCopyOptionsSkipMetadata(t *testing.T) {
	fsys := openCopyTestFS(t)
	writeStoreFile(t, fsys, "/src.txt", "data")
	require.NoError(t, fsys.Chmod("/src.txt", 0o600))
	past := time.Now().Add(-48 * time.Hour).Truncate(time.Second)
	require.NoError(t, fsys.Chtimes("/src.txt", past, past))

	// The default preserves mode and timestamps.
	require.NoError(t, fsys.Copy("/src.txt", "/kept.txt"))
	info, err := fsys.Stat("/kept.txt")
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	require.WithinDuration(t, past, info.ModTime(), time.Second)

	// SkipMetadata resets permissions and gives fresh timestamps.
	require.NoError(t, fsys.CopyWithOptions("/src.txt", "/fresh.txt", CopyOptions{SkipMetadata: true}))
	info, err = fsys.Stat("/fresh.txt")
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o644), info.Mode().Perm())
	require.False(t, info.ModTime().Before(past.Add(time.Hour)), "timestamps must not be copied")
	require.WithinDuration(t, time.Now(), info.ModTime(), time.Minute)

	// Directories get 0755 instead of the source's mode.
	require.NoError(t, fsys.Mkdir("/private", 0o700))
	require.NoError(t, fsys.CopyWithOptions("/private", "/public", CopyOptions{SkipMetadata: true}))
	info, err = fsys.Stat("/public")
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o755), info.Mode().Perm())
}

func TestCopyOptionsSkipSymlinks(t *testing.T) {
	fsys := openCopyTestFS(t)
	require.NoError(t, fsys.Mkdir("/src", 0o755))
	writeStoreFile(t, fsys, "/src/file.txt", "data")
	require.NoError(t, fsys.Symlink("/src/file.txt", "/src/link"))

	// The default recreates symlinks as symlinks.
	require.NoError(t, fsys.Copy("/src", "/default"))
	target, err := fsys.Readlink("/default/link")
	require.NoError(t, err)
	require.Equal(t, "/src/file.txt", target)

	// SkipSymlinks omits them but copies the rest.
	require.NoError(t, fsys.CopyWithOptions("/src", "/skipped", CopyOptions{SkipSymlinks: true}))
	_, err = fsys.Lstat("/skipped/link")
	require.True(t, os.IsNotExist(err))
	require.Equal(t, "data", readAll(t, fsys, "/skipped/file.txt"))

	// Copying a symlink itself with SkipSymlinks is rejected.
	require.ErrorIs(t, fsys.CopyWithOptions("/src/link", "/link-copy", CopyOptions{SkipSymlinks: true}), os.ErrInvalid)
}

func TestCopyContextProgress(t *testing.T) {
	fsys := openCopyTestFS(t)
	require.NoError(t, fsys.Mkdir("/src", 0o755))
	writeStoreFile(t, fsys, "/src/a.txt", "12345")
	writeStoreFile(t, fsys, "/src/b.txt", "1234567")

	var calls []int64
	require.NoError(t, fsys.CopyWithOptions("/src", "/dst", CopyOptions{
		Progress: func(copied int64) { calls = append(calls, copied) },
	}))

	require.Len(t, calls, 2)
	require.Equal(t, int64(12), calls[len(calls)-1])
	require.Less(t, calls[0], calls[1])
}

func TestCopyContextCancelKeepsDestination(t *testing.T) {
	fsys := openCopyTestFS(t)
	writeStoreFile(t, fsys, "/src.txt", "new")
	writeStoreFile(t, fsys, "/dst.txt", "old")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := fsys.CopyContext(ctx, "/src.txt", "/dst.txt", CopyOptions{
		Overwrite: true,
		Progress:  func(int64) { cancel() },
	})
	require.ErrorIs(t, err, context.Canceled)

	// The destination was not replaced by the aborted copy.
	require.Equal(t, "old", readAll(t, fsys, "/dst.txt"))

	// A newly created destination name does not appear.
	err = fsys.CopyContext(ctx, "/src.txt", "/new.txt", CopyOptions{})
	require.ErrorIs(t, err, context.Canceled)
	_, statErr := fsys.Stat("/new.txt")
	require.True(t, os.IsNotExist(statErr))
}

func TestCopyContextNilAndPreCancelled(t *testing.T) {
	fsys := openCopyTestFS(t)
	writeStoreFile(t, fsys, "/src.txt", "data")

	//lint:ignore SA1012 intentionally exercising the nil-context guard
	require.ErrorIs(t, fsys.CopyContext(nil, "/src.txt", "/dst.txt", CopyOptions{}), ErrNilContext)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.True(t, errors.Is(fsys.CopyContext(ctx, "/src.txt", "/dst.txt", CopyOptions{}), context.Canceled))
}
