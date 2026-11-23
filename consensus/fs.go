package consensus

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

	"github.com/malivvan/harbor/fs"
)

// FS is the replicated write view of a named file system in the fs sub-store.
type FS struct {
	n      *Node
	fsName string
}

type fsPathArg struct {
	FS    string      `json:"fs"`
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

// FS returns the replicated fs sub-store root.
func (n *Node) FS() *FS {
	if n == nil {
		return nil
	}
	return &FS{n: n}
}

// Open returns a replicated view of the named file system, creating it on
// demand.
func (f *FS) Open(name string) *FS {
	if f == nil || f.n == nil {
		return nil
	}
	return &FS{n: f.n, fsName: name}
}

// Mkdir replicates fs.FS.Mkdir. Times are pinned at proposal time so every
// replica records the same directory metadata.
func (f *FS) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	now := time.Now()
	_, err := f.n.apply(opFSMkdir, fsPathArg{FS: f.fsName, Name: name, Perm: perm, Atime: now, Mtime: now})
	return err
}

// Remove replicates fs.FS.Remove.
func (f *FS) Remove(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFSRemove, fsPathArg{FS: f.fsName, Name: name})
	return err
}

// RemoveAll replicates fs.FS.RemoveAll.
func (f *FS) RemoveAll(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFSRemoveAll, fsPathArg{FS: f.fsName, Name: path})
	return err
}

// Rename replicates fs.FS.Rename.
func (f *FS) Rename(ctx context.Context, oldpath, newpath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFSRename, fsPathArg{FS: f.fsName, Name: oldpath, New: newpath})
	return err
}

// WriteFile replicates a create-and-write of a whole file. Times are pinned at
// proposal time.
func (f *FS) WriteFile(ctx context.Context, name string, data []byte, perm os.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	now := time.Now()
	_, err := f.n.apply(opFSWrite, fsPathArg{FS: f.fsName, Name: name, Perm: perm, Atime: now, Mtime: now, Data: data})
	return err
}

// MkdirAll replicates fs.FS.MkdirAll. Every component missing on the replicas
// is created with the times pinned here, so directory metadata matches even
// though MkdirAll would otherwise stamp each replica's own clock.
func (f *FS) MkdirAll(ctx context.Context, name string, perm os.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	now := time.Now()
	_, err := f.n.apply(opFSMkdirAll, fsPathArg{FS: f.fsName, Name: name, Perm: perm, Atime: now, Mtime: now})
	return err
}

// Truncate replicates fs.FS.Truncate.
func (f *FS) Truncate(ctx context.Context, name string, size int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFSTruncate, fsPathArg{FS: f.fsName, Name: name, Size: size})
	return err
}

// Chmod replicates fs.FS.Chmod.
func (f *FS) Chmod(ctx context.Context, name string, mode os.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFSChmod, fsPathArg{FS: f.fsName, Name: name, Perm: mode})
	return err
}

// Chtimes replicates fs.FS.Chtimes. Both times are explicit, so replicas store
// exactly the values the caller supplied.
func (f *FS) Chtimes(ctx context.Context, name string, atime, mtime time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFSChtimes, fsPathArg{FS: f.fsName, Name: name, Atime: atime, Mtime: mtime})
	return err
}

// Chown replicates fs.FS.Chown. Pass -1 to leave a value unchanged, as the
// direct API does.
func (f *FS) Chown(ctx context.Context, name string, uid, gid int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFSChown, fsPathArg{FS: f.fsName, Name: name, Uid: uid, Gid: gid})
	return err
}

// Symlink replicates fs.FS.Symlink.
func (f *FS) Symlink(ctx context.Context, source, destination string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f == nil || f.n == nil {
		return ErrClosed
	}
	_, err := f.n.apply(opFSSymlink, fsPathArg{FS: f.fsName, Name: source, New: destination})
	return err
}

// Create returns a replicated file handle for name, truncating it on Close.
// Writes buffer in memory and one command replicates the complete content, so
// a partial file never enters the raft log. Use WriteFile for small content
// that is already in memory.
func (f *FS) Create(ctx context.Context, name string, perm os.FileMode) (*File, error) {
	return f.newFile(ctx, name, perm, false)
}

// OpenWriter returns a replicated file handle positioned at the end of the
// current content of name, which is read from the leader's replica. Close
// replicates the complete content. A missing file is created on Close.
//
// Writes made through another replica or a direct store between OpenWriter and
// Close are not observed; Close replaces the content with this handle's buffer.
func (f *FS) OpenWriter(ctx context.Context, name string, perm os.FileMode) (*File, error) {
	return f.newFile(ctx, name, perm, true)
}

func (f *FS) newFile(ctx context.Context, name string, perm os.FileMode, appendMode bool) (*File, error) {
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
	file := &File{n: f.n, fsName: f.fsName, name: name, perm: perm}
	if appendMode {
		fsys, err := st.FS().Open(f.fsName)
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

// File is a replicated file handle. Writes are buffered in memory; Close
// replicates the whole content in one command. It is safe for concurrent use.
type File struct {
	mu     sync.Mutex
	n      *Node
	fsName string
	name   string
	perm   os.FileMode
	data   []byte
	off    int64
	closed bool
}

// Write appends p at the current offset, like writing to an open file.
func (f *File) Write(p []byte) (int, error) {
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
func (f *File) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("consensus: write at negative offset: %w", os.ErrInvalid)
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
func (f *File) Truncate(size int64) error {
	if size < 0 {
		return fmt.Errorf("consensus: truncate to negative size: %w", os.ErrInvalid)
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
func (f *File) Size() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int64(len(f.data))
}

// Close replicates the complete buffered content and marks the handle closed.
// It is idempotent; after a failed apply the handle stays open so Close can be
// retried.
func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	if f.n == nil {
		return ErrClosed
	}
	now := time.Now()
	_, err := f.n.apply(opFSWrite, fsPathArg{
		FS:    f.fsName,
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

func (f *File) grow(size int64) {
	if int64(len(f.data)) >= size {
		return
	}
	f.data = append(f.data, make([]byte, size-int64(len(f.data)))...)
}

// DeleteFS replicates fs.Store.DeleteFS.
func (n *Node) DeleteFS(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if n == nil {
		return ErrClosed
	}
	_, err := n.apply(opFSDeleteFS, fsPathArg{FS: name})
	return err
}

func (f *fsm) openFS(a fsPathArg) (*fs.FS, error) {
	st := f.node.storeHandle()
	if st == nil {
		return nil, ErrClosed
	}
	fsys, err := st.FS().Open(a.FS)
	if err != nil {
		return nil, err
	}
	return fsys, nil
}

func (f *fsm) applyFSMkdir(a fsPathArg) any {
	fsys, err := f.openFS(a)
	if err != nil {
		return err
	}
	if err := fsys.Mkdir(a.Name, a.Perm); err != nil {
		return err
	}
	return fsys.Chtimes(a.Name, a.Atime, a.Mtime)
}

// applyFSMkdirAll creates missing components one at a time so the pinned times
// apply to every directory this command creates. fs.MkdirAll would stamp each
// intermediate directory with the replica's own clock instead.
func (f *fsm) applyFSMkdirAll(a fsPathArg) any {
	fsys, err := f.openFS(a)
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

func (f *fsm) applyFSTruncate(a fsPathArg) any {
	fsys, err := f.openFS(a)
	if err != nil {
		return err
	}
	return fsys.Truncate(a.Name, a.Size)
}

func (f *fsm) applyFSChmod(a fsPathArg) any {
	fsys, err := f.openFS(a)
	if err != nil {
		return err
	}
	return fsys.Chmod(a.Name, a.Perm)
}

func (f *fsm) applyFSChtimes(a fsPathArg) any {
	fsys, err := f.openFS(a)
	if err != nil {
		return err
	}
	return fsys.Chtimes(a.Name, a.Atime, a.Mtime)
}

func (f *fsm) applyFSChown(a fsPathArg) any {
	fsys, err := f.openFS(a)
	if err != nil {
		return err
	}
	return fsys.Chown(a.Name, a.Uid, a.Gid)
}

func (f *fsm) applyFSSymlink(a fsPathArg) any {
	fsys, err := f.openFS(a)
	if err != nil {
		return err
	}
	return fsys.Symlink(a.Name, a.New)
}

func (f *fsm) applyFSRemove(a fsPathArg) any {
	fsys, err := f.openFS(a)
	if err != nil {
		return err
	}
	return fsys.Remove(a.Name)
}

func (f *fsm) applyFSRemoveAll(a fsPathArg) any {
	fsys, err := f.openFS(a)
	if err != nil {
		return err
	}
	return fsys.RemoveAll(a.Name)
}

func (f *fsm) applyFSRename(a fsPathArg) any {
	fsys, err := f.openFS(a)
	if err != nil {
		return err
	}
	return fsys.Rename(a.Name, a.New)
}

func (f *fsm) applyFSWrite(a fsPathArg) any {
	fsys, err := f.openFS(a)
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

func (f *fsm) applyFSDeleteFS(a fsPathArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.FS().DeleteFS(a.FS)
}
