package consensus

import (
	"context"
	"os"
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
