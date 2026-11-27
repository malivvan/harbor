package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/malivvan/tempest/store/fstree"
)

// File is the replicated write view of a named file system in the fstree sub-store.
type File struct {
	n        *Node
	fileName string
}

type filePathArg struct {
	File  string      `json:"file"`
	Name  string      `json:"name"`
	New   string      `json:"new,omitempty"`
	Perm  os.FileMode `json:"perm"`
	Atime time.Time   `json:"atime"`
	Mtime time.Time   `json:"mtime"`
	Data  []byte      `json:"data,omitempty"`
	Size  int64       `json:"size,omitempty"`
	Uid   int         `json:"uid,omitempty"`
	Gid   int         `json:"gid,omitempty"`
}

// File returns the replicated fstree sub-store root.
func (n *Node) File() *File {
	if n == nil {
		return nil
	}
	return &File{n: n}
}

// Open returns a replicated view of the named file system, creating it on
// demand.
func (f *File) Open(name string) *File {
	if f == nil || f.n == nil {
		return nil
	}
	return &File{n: f.n, fileName: name}
}

// Mkdir replicates fs.File.Mkdir. Times are pinned at proposal time so every
// replica records the same directory metadata.
func (f *File) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	now := time.Now()
	_, err := f.n.apply(opFileMkdir, filePathArg{File: f.fileName, Name: name, Perm: perm, Atime: now, Mtime: now})
	return err
}

// Remove replicates fs.File.Remove.
func (f *File) Remove(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFileRemove, filePathArg{File: f.fileName, Name: name})
	return err
}

// RemoveAll replicates fs.File.RemoveAll.
func (f *File) RemoveAll(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFileRemoveAll, filePathArg{File: f.fileName, Name: path})
	return err
}

// Rename replicates fs.File.Rename.
func (f *File) Rename(ctx context.Context, oldpath, newpath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFileRename, filePathArg{File: f.fileName, Name: oldpath, New: newpath})
	return err
}

// WriteFile replicates a create-and-write of a whole file. Times are pinned at
// proposal time.
func (f *File) WriteFile(ctx context.Context, name string, data []byte, perm os.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	now := time.Now()
	_, err := f.n.apply(opFileWrite, filePathArg{File: f.fileName, Name: name, Perm: perm, Atime: now, Mtime: now, Data: data})
	return err
}

// MkdirAll replicates fs.File.MkdirAll. Every component missing on the replicas
// is created with the times pinned here, so directory metadata matches even
// though MkdirAll would otherwise stamp each replica's own clock.
func (f *File) MkdirAll(ctx context.Context, name string, perm os.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	now := time.Now()
	_, err := f.n.apply(opFileMkdirAll, filePathArg{File: f.fileName, Name: name, Perm: perm, Atime: now, Mtime: now})
	return err
}

// Truncate replicates fs.File.Truncate.
func (f *File) Truncate(ctx context.Context, name string, size int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFileTruncate, filePathArg{File: f.fileName, Name: name, Size: size})
	return err
}

// Chmod replicates fs.File.Chmod.
func (f *File) Chmod(ctx context.Context, name string, mode os.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFileChmod, filePathArg{File: f.fileName, Name: name, Perm: mode})
	return err
}

// Chtimes replicates fs.File.Chtimes. Both times are explicit, so replicas store
// exactly the values the caller supplied.
func (f *File) Chtimes(ctx context.Context, name string, atime, mtime time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFileChtimes, filePathArg{File: f.fileName, Name: name, Atime: atime, Mtime: mtime})
	return err
}

// Chown replicates fs.File.Chown. Pass -1 to leave a value unchanged, as the
// direct API does.
func (f *File) Chown(ctx context.Context, name string, uid, gid int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFileChown, filePathArg{File: f.fileName, Name: name, Uid: uid, Gid: gid})
	return err
}

// Symlink replicates fs.File.Symlink.
func (f *File) Symlink(ctx context.Context, source, destination string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFileSymlink, filePathArg{File: f.fileName, Name: source, New: destination})
	return err
}

// Create returns a replicated file handle for name, truncating it on Close.
// Writes buffer in memory and one command replicates the complete content, so
// a partial file never enters the raft log. Use WriteFile for small content
// that is already in memory.
func (f *File) Create(ctx context.Context, name string, perm os.FileMode) (*FileHandle, error) {
	return f.newFile(ctx, name, perm, false)
}

// OpenWriter returns a replicated file handle positioned at the end of the
// current content of name, which is read from the leader's replica. Close
// replicates the complete content. A missing file is created on Close.
//
// Writes made through another replica or a direct store between OpenWriter and
// Close are not observed; Close replaces the content with this handle's buffer.
func (f *File) OpenWriter(ctx context.Context, name string, perm os.FileMode) (*FileHandle, error) {
	return f.newFile(ctx, name, perm, true)
}

func (f *File) newFile(ctx context.Context, name string, perm os.FileMode, appendMode bool) (*FileHandle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f == nil || f.n == nil {
		return nil, ErrClosed
	}
	st := f.n.storeHandle()
	if st == nil {
		return nil, ErrClosed
	}
	file := &FileHandle{n: f.n, fileName: f.fileName, name: name, perm: perm}
	if appendMode {
		fsys, err := st.File().Open(f.fileName)
		if err != nil {
			return nil, err
		}
		data, err := fsys.ReadFile(name)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		file.data = data
	}
	file.off = int64(len(file.data))
	return file, nil
}

// FileHandle is a replicated file handle. Writes are buffered in memory; Close
// replicates the whole content in one command. It is safe for concurrent use.
type FileHandle struct {
	mu       sync.Mutex
	n        *Node
	fileName string
	name     string
	perm     os.FileMode
	data     []byte
	off      int64
	closed   bool
}

// Write appends p at the current offset, like writing to an open file.
func (f *FileHandle) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, ErrClosed
	}
	f.grow(f.off + int64(len(p)))
	copy(f.data[f.off:], p)
	f.off += int64(len(p))
	return len(p), nil
}

// WriteAt writes p at off, growing the buffer with zero bytes as needed.
func (f *FileHandle) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("cluster: write at negative offset: %w", os.ErrInvalid)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, ErrClosed
	}
	f.grow(off + int64(len(p)))
	copy(f.data[off:], p)
	if end := off + int64(len(p)); end > f.off {
		f.off = end
	}
	return len(p), nil
}

// Truncate resizes the buffered content.
func (f *FileHandle) Truncate(size int64) error {
	if size < 0 {
		return fmt.Errorf("cluster: truncate to negative size: %w", os.ErrInvalid)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrClosed
	}
	switch {
	case size < int64(len(f.data)):
		f.data = f.data[:size]
	case size > int64(len(f.data)):
		f.grow(size)
	}
	if f.off > size {
		f.off = size
	}
	return nil
}

// Size returns the buffered content length.
func (f *FileHandle) Size() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int64(len(f.data))
}

// Close replicates the complete buffered content and marks the handle closed.
// It is idempotent; after a failed apply the handle stays open so Close can be
// retried.
func (f *FileHandle) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	if f.n == nil {
		return ErrClosed
	}
	now := time.Now()
	_, err := f.n.apply(opFileWrite, filePathArg{
		File:  f.fileName,
		Name:  f.name,
		Perm:  f.perm,
		Atime: now,
		Mtime: now,
		Data:  f.data,
	})
	if err != nil {
		return err
	}
	f.closed = true
	return nil
}

func (f *FileHandle) grow(size int64) {
	if int64(len(f.data)) >= size {
		return
	}
	f.data = append(f.data, make([]byte, size-int64(len(f.data)))...)
}

// DeleteFileSystem replicates fs.Store.DeleteFileSystem.
func (n *Node) DeleteFileSystem(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if n == nil {
		return ErrClosed
	}
	_, err := n.apply(opFileDeleteFileSystem, filePathArg{File: name})
	return err
}

func (f *fsm) openFileSystem(a filePathArg) (*fstree.FS, error) {
	st := f.node.storeHandle()
	if st == nil {
		return nil, ErrClosed
	}
	fsys, err := st.File().Open(a.File)
	if err != nil {
		return nil, err
	}
	return fsys, nil
}

func (f *fsm) applyFileMkdir(a filePathArg) any {
	fsys, err := f.openFileSystem(a)
	if err != nil {
		return err
	}
	if err := fsys.Mkdir(a.Name, a.Perm); err != nil {
		return err
	}
	return fsys.Chtimes(a.Name, a.Atime, a.Mtime)
}

// applyFileMkdirAll creates missing components one at a time so the pinned times
// apply to every directory this command creates. fs.MkdirAll would stamp each
// intermediate directory with the replica's own clock instead.
func (f *fsm) applyFileMkdirAll(a filePathArg) any {
	fsys, err := f.openFileSystem(a)
	if err != nil {
		return err
	}
	full := path.Clean(a.Name)
	if !strings.HasPrefix(full, "/") {
		full = "/" + full
	}
	dir := "/"
	for _, part := range strings.Split(strings.TrimPrefix(full, "/"), "/") {
		if part == "" {
			continue
		}
		dir = path.Join(dir, part)
		info, statErr := fsys.Stat(dir)
		if statErr == nil {
			if !info.IsDir() {
				return &os.PathError{Op: "mkdir", Path: dir, Err: syscall.ENOTDIR}
			}
			continue
		}
		if !errors.Is(statErr, os.ErrNotExist) {
			return statErr
		}
		if err := fsys.Mkdir(dir, a.Perm); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		if err := fsys.Chtimes(dir, a.Atime, a.Mtime); err != nil {
			return err
		}
	}
	return nil
}

func (f *fsm) applyFileTruncate(a filePathArg) any {
	fsys, err := f.openFileSystem(a)
	if err != nil {
		return err
	}
	return fsys.Truncate(a.Name, a.Size)
}

func (f *fsm) applyFileChmod(a filePathArg) any {
	fsys, err := f.openFileSystem(a)
	if err != nil {
		return err
	}
	return fsys.Chmod(a.Name, a.Perm)
}

func (f *fsm) applyFileChtimes(a filePathArg) any {
	fsys, err := f.openFileSystem(a)
	if err != nil {
		return err
	}
	return fsys.Chtimes(a.Name, a.Atime, a.Mtime)
}

func (f *fsm) applyFileChown(a filePathArg) any {
	fsys, err := f.openFileSystem(a)
	if err != nil {
		return err
	}
	return fsys.Chown(a.Name, a.Uid, a.Gid)
}

func (f *fsm) applyFileSymlink(a filePathArg) any {
	fsys, err := f.openFileSystem(a)
	if err != nil {
		return err
	}
	return fsys.Symlink(a.Name, a.New)
}

func (f *fsm) applyFileRemove(a filePathArg) any {
	fsys, err := f.openFileSystem(a)
	if err != nil {
		return err
	}
	return fsys.Remove(a.Name)
}

func (f *fsm) applyFileRemoveAll(a filePathArg) any {
	fsys, err := f.openFileSystem(a)
	if err != nil {
		return err
	}
	return fsys.RemoveAll(a.Name)
}

func (f *fsm) applyFileRename(a filePathArg) any {
	fsys, err := f.openFileSystem(a)
	if err != nil {
		return err
	}
	return fsys.Rename(a.Name, a.New)
}

func (f *fsm) applyFileWrite(a filePathArg) any {
	fsys, err := f.openFileSystem(a)
	if err != nil {
		return err
	}
	file, err := fsys.OpenFile(a.Name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, a.Perm)
	if err != nil {
		return err
	}
	if _, err := file.Write(a.Data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return fsys.Chtimes(a.Name, a.Atime, a.Mtime)
}

func (f *fsm) applyFileDeleteFileSystem(a filePathArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.File().DeleteFS(a.File)
}
