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

// Fstree is the replicated write view of a named file system in the fstree sub-store.
type Fstree struct {
	n          *Node
	fstreeName string
}

type fstreePathArg struct {
	Fstree string      `json:"fstree"`
	Name   string      `json:"name"`
	New    string      `json:"new,omitempty"`
	Perm   os.FileMode `json:"perm"`
	Atime  time.Time   `json:"atime"`
	Mtime  time.Time   `json:"mtime"`
	Data   []byte      `json:"data,omitempty"`
	Size   int64       `json:"size,omitempty"`
	Uid    int         `json:"uid,omitempty"`
	Gid    int         `json:"gid,omitempty"`
}

// Fstree returns the replicated fstree sub-store root.
func (n *Node) Fstree() *Fstree {
	if n == nil {
		return nil
	}
	return &Fstree{n: n}
}

// Open returns a replicated view of the named file system, creating it on
// demand.
func (f *Fstree) Open(name string) *Fstree {
	if f == nil || f.n == nil {
		return nil
	}
	return &Fstree{n: f.n, fstreeName: name}
}

// Mkdir replicates fstree.FS.Mkdir. Times are pinned at proposal time so every
// replica records the same directory metadata.
func (f *Fstree) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	now := time.Now()
	_, err := f.n.apply(opFstreeMkdir, fstreePathArg{Fstree: f.fstreeName, Name: name, Perm: perm, Atime: now, Mtime: now})
	return err
}

// Remove replicates fstree.FS.Remove.
func (f *Fstree) Remove(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFstreeRemove, fstreePathArg{Fstree: f.fstreeName, Name: name})
	return err
}

// RemoveAll replicates fstree.FS.RemoveAll.
func (f *Fstree) RemoveAll(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFstreeRemoveAll, fstreePathArg{Fstree: f.fstreeName, Name: path})
	return err
}

// Rename replicates fstree.FS.Rename.
func (f *Fstree) Rename(ctx context.Context, oldpath, newpath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFstreeRename, fstreePathArg{Fstree: f.fstreeName, Name: oldpath, New: newpath})
	return err
}

// WriteFile replicates a create-and-write of a whole file. Times are pinned at
// proposal time.
func (f *Fstree) WriteFile(ctx context.Context, name string, data []byte, perm os.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	now := time.Now()
	_, err := f.n.apply(opFstreeWrite, fstreePathArg{Fstree: f.fstreeName, Name: name, Perm: perm, Atime: now, Mtime: now, Data: data})
	return err
}

// MkdirAll replicates fstree.FS.MkdirAll. Every component missing on the replicas
// is created with the times pinned here, so directory metadata matches even
// though MkdirAll would otherwise stamp each replica's own clock.
func (f *Fstree) MkdirAll(ctx context.Context, name string, perm os.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	now := time.Now()
	_, err := f.n.apply(opFstreeMkdirAll, fstreePathArg{Fstree: f.fstreeName, Name: name, Perm: perm, Atime: now, Mtime: now})
	return err
}

// Truncate replicates fstree.FS.Truncate.
func (f *Fstree) Truncate(ctx context.Context, name string, size int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFstreeTruncate, fstreePathArg{Fstree: f.fstreeName, Name: name, Size: size})
	return err
}

// Chmod replicates fstree.FS.Chmod.
func (f *Fstree) Chmod(ctx context.Context, name string, mode os.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFstreeChmod, fstreePathArg{Fstree: f.fstreeName, Name: name, Perm: mode})
	return err
}

// Chtimes replicates fstree.FS.Chtimes. Both times are explicit, so replicas store
// exactly the values the caller supplied.
func (f *Fstree) Chtimes(ctx context.Context, name string, atime, mtime time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFstreeChtimes, fstreePathArg{Fstree: f.fstreeName, Name: name, Atime: atime, Mtime: mtime})
	return err
}

// Chown replicates fstree.FS.Chown. Pass -1 to leave a value unchanged, as the
// direct API does.
func (f *Fstree) Chown(ctx context.Context, name string, uid, gid int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFstreeChown, fstreePathArg{Fstree: f.fstreeName, Name: name, Uid: uid, Gid: gid})
	return err
}

// Symlink replicates fstree.FS.Symlink.
func (f *Fstree) Symlink(ctx context.Context, source, destination string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFstreeSymlink, fstreePathArg{Fstree: f.fstreeName, Name: source, New: destination})
	return err
}

// Create returns a replicated file handle for name, truncating it on Close.
// Writes buffer in memory and one command replicates the complete content, so
// a partial file never enters the raft log. Use WriteFile for small content
// that is already in memory.
func (f *Fstree) Create(ctx context.Context, name string, perm os.FileMode) (*FileHandle, error) {
	return f.newFile(ctx, name, perm, false)
}

// OpenWriter returns a replicated file handle positioned at the end of the
// current content of name, which is read from the leader's replica. Close
// replicates the complete content. A missing file is created on Close.
//
// Writes made through another replica or a direct store between OpenWriter and
// Close are not observed; Close replaces the content with this handle's buffer.
func (f *Fstree) OpenWriter(ctx context.Context, name string, perm os.FileMode) (*FileHandle, error) {
	return f.newFile(ctx, name, perm, true)
}

func (f *Fstree) newFile(ctx context.Context, name string, perm os.FileMode, appendMode bool) (*FileHandle, error) {
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
	file := &FileHandle{n: f.n, fstreeName: f.fstreeName, name: name, perm: perm}
	if appendMode {
		fsys, err := st.Fstree().Open(f.fstreeName)
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
	mu         sync.Mutex
	n          *Node
	fstreeName string
	name       string
	perm       os.FileMode
	data       []byte
	off        int64
	closed     bool
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
	_, err := f.n.apply(opFstreeWrite, fstreePathArg{
		Fstree: f.fstreeName,
		Name:   f.name,
		Perm:   f.perm,
		Atime:  now,
		Mtime:  now,
		Data:   f.data,
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
	_, err := n.apply(opFstreeDeleteFileSystem, fstreePathArg{Fstree: name})
	return err
}

func (f *fsm) openFileSystem(a fstreePathArg) (*fstree.FS, error) {
	st := f.node.storeHandle()
	if st == nil {
		return nil, ErrClosed
	}
	fsys, err := st.Fstree().Open(a.Fstree)
	if err != nil {
		return nil, err
	}
	return fsys, nil
}

func (f *fsm) applyFileMkdir(a fstreePathArg) any {
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
func (f *fsm) applyFileMkdirAll(a fstreePathArg) any {
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

func (f *fsm) applyFileTruncate(a fstreePathArg) any {
	fsys, err := f.openFileSystem(a)
	if err != nil {
		return err
	}
	return fsys.Truncate(a.Name, a.Size)
}

func (f *fsm) applyFileChmod(a fstreePathArg) any {
	fsys, err := f.openFileSystem(a)
	if err != nil {
		return err
	}
	return fsys.Chmod(a.Name, a.Perm)
}

func (f *fsm) applyFileChtimes(a fstreePathArg) any {
	fsys, err := f.openFileSystem(a)
	if err != nil {
		return err
	}
	return fsys.Chtimes(a.Name, a.Atime, a.Mtime)
}

func (f *fsm) applyFileChown(a fstreePathArg) any {
	fsys, err := f.openFileSystem(a)
	if err != nil {
		return err
	}
	return fsys.Chown(a.Name, a.Uid, a.Gid)
}

func (f *fsm) applyFileSymlink(a fstreePathArg) any {
	fsys, err := f.openFileSystem(a)
	if err != nil {
		return err
	}
	return fsys.Symlink(a.Name, a.New)
}

func (f *fsm) applyFileRemove(a fstreePathArg) any {
	fsys, err := f.openFileSystem(a)
	if err != nil {
		return err
	}
	return fsys.Remove(a.Name)
}

func (f *fsm) applyFileRemoveAll(a fstreePathArg) any {
	fsys, err := f.openFileSystem(a)
	if err != nil {
		return err
	}
	return fsys.RemoveAll(a.Name)
}

func (f *fsm) applyFileRename(a fstreePathArg) any {
	fsys, err := f.openFileSystem(a)
	if err != nil {
		return err
	}
	return fsys.Rename(a.Name, a.New)
}

func (f *fsm) applyFileWrite(a fstreePathArg) any {
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

func (f *fsm) applyFileDeleteFileSystem(a fstreePathArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Fstree().DeleteFS(a.Fstree)
}
