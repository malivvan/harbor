package fs

import (
	"io"
	"io/fs"
	"os"
	"path"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/malivvan/storm/internal/absfs"
)

// Snapshot represents a read-only point-in-time view of the filesystem.
//
// The metadata and content are materialized into memory when the snapshot is
// created, and the bbolt read transaction is released immediately. Holding a
// read transaction for the lifetime of a snapshot pins the database mmap: a
// later write that needs to remap (for example CopyToFS writing to the same
// database) would block forever waiting for the reader. Materializing also
// means snapshots do not pin the MVCC view or prevent the database file from
// growing. Snapshots are therefore non-durable and their memory cost is
// proportional to the size of the filesystem.
//
// When the filesystem uses an external contentFS, the snapshot isolates the
// bbolt metadata (names, inodes, chunked content) but external content is read
// live. A concurrent write or delete in the external store is visible through
// the snapshot; use an immutable/versioned external backend if point-in-time
// content is required.
type Snapshot struct {
	fs         *FS
	nodes      map[uint64]*iNode
	content    map[uint64][]byte
	contentErr map[uint64]error
	links      map[uint64]string
	created    time.Time
	name       string
	released   bool
}

// CreateSnapshot creates a new read-only snapshot of the filesystem.
// The snapshot must be released by calling Release() when done to free memory.
func (fsys *FS) CreateSnapshot(name string) (*Snapshot, error) {
	tx, err := fsys.db.Begin(false) // false = read-only transaction
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() // read-only: Rollback is the close operation

	bucket, err := fsys.openFsBucket(tx) // Snapshots don't use the cache
	if err != nil {
		return nil, err
	}

	return fsys.materializeSnapshot(bucket, name)
}

// materializeSnapshot copies the inode table, file content and symlink targets
// into private buffers so the snapshot no longer needs the read transaction.
func (fsys *FS) materializeSnapshot(bucket *fsBucket, name string) (*Snapshot, error) {
	snap := &Snapshot{
		fs:         fsys,
		nodes:      make(map[uint64]*iNode),
		content:    make(map[uint64][]byte),
		contentErr: make(map[uint64]error),
		links:      make(map[uint64]string),
		created:    time.Now(),
		name:       name,
	}

	if err := bucket.inodes.ForEach(func(k, v []byte) error {
		if len(k) != 8 || len(v) == 0 {
			return nil
		}
		ino := b2i(k)
		node := newInode(0)
		if err := bucket.decodeNode(ino, node); err != nil {
			return err
		}
		snap.nodes[ino] = node
		return nil
	}); err != nil {
		return nil, err
	}

	// External content is deliberately left live; see the type comment.
	if fsys.contentFS == nil {
		for ino, node := range snap.nodes {
			if !node.Mode.IsRegular() || node.Size <= 0 {
				continue
			}
			if fsys.chunked() {
				data, err := bucket.readDataRange(ino, 0, node.Size, fsys.chunkSize())
				if err != nil {
					// Do not let one corrupt file abort snapshot creation;
					// surface the error when that file is read.
					snap.contentErr[ino] = err
					continue
				}
				snap.content[ino] = data
				continue
			}
			raw := bucket.data.Get(i2b(ino))
			if raw == nil {
				snap.content[ino] = []byte{}
				continue
			}
			plain, err := bucket.enc.openContent(raw)
			if err != nil {
				// Surface the corruption when this file is read instead of
				// failing the whole snapshot.
				snap.contentErr[ino] = err
				continue
			}
			// raw aliases the mmap; keep a private copy.
			snap.content[ino] = append([]byte(nil), plain...)
		}
	}

	if err := bucket.symlinks.ForEach(func(k, v []byte) error {
		if len(k) != 8 || v == nil {
			return nil
		}
		plain, err := bucket.enc.openContent(v)
		if err != nil {
			return err
		}
		snap.links[b2i(k)] = string(plain)
		return nil
	}); err != nil {
		return nil, err
	}

	return snap, nil
}

// Name returns the snapshot's name.
func (s *Snapshot) Name() string {
	return s.name
}

// Created returns the time when the snapshot was created.
func (s *Snapshot) Created() time.Time {
	return s.created
}

// Release frees the snapshot and its in-memory state.
// The snapshot cannot be used after calling Release().
func (s *Snapshot) Release() error {
	if s.released {
		return nil
	}
	s.released = true
	s.nodes = nil
	s.content = nil
	s.contentErr = nil
	s.links = nil
	return nil
}

// resolve resolves a path within the snapshot to an iNode.
func (s *Snapshot) resolve(path string) (*iNode, error) {
	if s.released {
		return nil, os.ErrClosed
	}

	ino := s.fs.rootIno

	loadedNode, err := s.inode(ino)
	if err != nil {
		return nil, err
	}
	node := *loadedNode

	if path == "/" {
		return &node, nil
	}

	for _, name := range strings.Split(strings.TrimLeft(path, "/"), "/") {
		// find the child's ino or error
		x := sort.Search(len(node.Children), func(i int) bool {
			return node.Children[i].Name >= name
		})
		if x == len(node.Children) || node.Children[x].Name != name {
			return nil, os.ErrNotExist
		}

		// replace node with child or error
		loadedNode, err = s.inode(node.Children[x].Ino)
		if err != nil {
			return nil, err
		}
		node = *loadedNode
	}

	return &node, nil
}

// inode returns a copy of the materialized inode table entry for ino.
func (s *Snapshot) inode(ino uint64) (*iNode, error) {
	node, ok := s.nodes[ino]
	if !ok {
		return nil, os.ErrNotExist
	}
	copied := *node
	return &copied, nil
}

// Stat returns file information for the given path in the snapshot.
func (s *Snapshot) Stat(name string) (os.FileInfo, error) {
	if s.released {
		return nil, os.ErrClosed
	}

	dir, filename := s.fs.cleanPath(name)
	p := path.Join(dir, filename)

	node, err := s.resolve(p)
	if err != nil {
		return nil, &os.PathError{Op: "stat", Path: name, Err: err}
	}

	return inodeinfo{name: filename, node: node}, nil
}

// ReadDir reads the directory named by path in the snapshot and returns
// a list of directory entries.
func (s *Snapshot) ReadDir(name string) ([]fs.DirEntry, error) {
	if s.released {
		return nil, os.ErrClosed
	}

	dir, filename := s.fs.cleanPath(name)
	p := path.Join(dir, filename)

	node, err := s.resolve(p)
	if err != nil {
		return nil, &os.PathError{Op: "readdir", Path: name, Err: err}
	}

	if !node.IsDir() {
		return nil, &os.PathError{Op: "readdir", Path: name, Err: syscall.ENOTDIR}
	}

	var entries []fs.DirEntry
	for _, child := range node.Children {
		childNode, err := s.inode(child.Ino)
		if err != nil {
			continue // Skip entries we can't read
		}
		entries = append(entries, &dirEntry{name: child.Name, node: childNode})
	}

	return entries, nil
}

// ReadFile reads the entire file at the given path in the snapshot.
func (s *Snapshot) ReadFile(name string) ([]byte, error) {
	if s.released {
		return nil, os.ErrClosed
	}

	dir, filename := s.fs.cleanPath(name)
	p := path.Join(dir, filename)

	node, err := s.resolve(p)
	if err != nil {
		return nil, &os.PathError{Op: "read", Path: name, Err: err}
	}

	if node.IsDir() {
		return nil, &os.PathError{Op: "read", Path: name, Err: syscall.EISDIR}
	}

	data, fresh, err := s.readContent(node)
	if err != nil {
		return nil, &os.PathError{Op: "read", Path: name, Err: err}
	}

	if fresh {
		// The content was decoded or read into a private buffer.
		return data, nil
	}

	// Plaintext bbolt content aliases the snapshot's mmap; return a private
	// copy so callers cannot modify database pages.
	result := make([]byte, len(data))
	copy(result, data)
	return result, nil
}

// readContent returns the content of node. External content is read live;
// everything else comes from the materialized snapshot.
// readContent returns the whole content of node and whether the returned
// buffer is private to the caller (and therefore safe to mutate).
func (s *Snapshot) readContent(node *iNode) ([]byte, bool, error) {
	if s.fs.contentFS != nil {
		// Content lives outside bbolt in the external content filesystem.
		data, err := s.fs.loadData(node.Ino)
		return data, true, err
	}

	if err, ok := s.contentErr[node.Ino]; ok {
		return nil, false, err
	}
	data, ok := s.content[node.Ino]
	if !ok {
		// File exists but has no data (empty file)
		return []byte{}, true, nil
	}
	// Return a private copy so callers cannot modify the snapshot.
	result := make([]byte, len(data))
	copy(result, data)
	return result, true, nil
}

// Readlink reads the target of a symbolic link in the snapshot.
func (s *Snapshot) Readlink(name string) (string, error) {
	if s.released {
		return "", os.ErrClosed
	}

	dir, filename := s.fs.cleanPath(name)
	p := path.Join(dir, filename)

	node, err := s.resolve(p)
	if err != nil {
		return "", &os.PathError{Op: "readlink", Path: name, Err: err}
	}

	if node.Mode&os.ModeSymlink == 0 {
		return "", &os.PathError{Op: "readlink", Path: name, Err: os.ErrInvalid}
	}

	target := s.links[node.Ino]
	return target, nil
}

// CopyToFS copies a file or directory from the snapshot to the main filesystem.
func (s *Snapshot) CopyToFS(srcPath, dstPath string) error {
	if s.released {
		return os.ErrClosed
	}

	// Get source info from snapshot
	info, err := s.Stat(srcPath)
	if err != nil {
		return err
	}

	if info.IsDir() {
		// Create directory in main filesystem
		err = s.fs.MkdirAll(dstPath, info.Mode())
		if err != nil {
			return err
		}

		// Recursively copy children
		children, err := s.ReadDir(srcPath)
		if err != nil {
			return err
		}

		for _, child := range children {
			childSrc := path.Join(srcPath, child.Name())
			childDst := path.Join(dstPath, child.Name())
			err = s.CopyToFS(childSrc, childDst)
			if err != nil {
				return err
			}
		}
	} else {
		// Copy file
		data, err := s.ReadFile(srcPath)
		if err != nil {
			return err
		}

		file, err := s.fs.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode())
		if err != nil {
			return err
		}

		_, err = file.Write(data)
		if err != nil {
			_ = file.Close()
			return err
		}
		// Never discard the Close error: an external/S3 content store commits
		// the object there.
		if err := file.Close(); err != nil {
			return err
		}
	}

	// Set times
	sys := info.Sys().(*iNode)
	return s.fs.Chtimes(dstPath, sys.Atime(), sys.Mtime())
}

// SnapshotManager manages named snapshots for a filesystem.
type SnapshotManager struct {
	fs        *FS
	snapshots map[string]*Snapshot
}

// NewSnapshotManager creates a new snapshot manager for the given filesystem.
func (fsys *FS) NewSnapshotManager() *SnapshotManager {
	return &SnapshotManager{
		fs:        fsys,
		snapshots: make(map[string]*Snapshot),
	}
}

// Create creates a new named snapshot.
func (sm *SnapshotManager) Create(name string) (*Snapshot, error) {
	if _, exists := sm.snapshots[name]; exists {
		return nil, os.ErrExist
	}

	snap, err := sm.fs.CreateSnapshot(name)
	if err != nil {
		return nil, err
	}

	sm.snapshots[name] = snap
	return snap, nil
}

// Get retrieves a snapshot by name.
func (sm *SnapshotManager) Get(name string) (*Snapshot, bool) {
	snap, ok := sm.snapshots[name]
	return snap, ok
}

// Delete releases and removes a snapshot by name.
func (sm *SnapshotManager) Delete(name string) error {
	snap, ok := sm.snapshots[name]
	if !ok {
		return os.ErrNotExist
	}

	err := snap.Release()
	delete(sm.snapshots, name)
	return err
}

// List returns a list of all snapshot names.
func (sm *SnapshotManager) List() []string {
	names := make([]string, 0, len(sm.snapshots))
	for name := range sm.snapshots {
		names = append(names, name)
	}
	return names
}

// ReleaseAll releases all snapshots.
func (sm *SnapshotManager) ReleaseAll() error {
	var firstErr error
	for name, snap := range sm.snapshots {
		if err := snap.Release(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(sm.snapshots, name)
	}
	return firstErr
}

// Ensure Snapshot implements relevant absfs interfaces where applicable.
var _ absfs.FileSystem = (*Snapshot)(nil)

// The following methods make Snapshot compatible with absfs.FS
// for read-only operations. Write operations return errors.

func (s *Snapshot) Chdir(dir string) error { return os.ErrPermission }
func (s *Snapshot) Getwd() (string, error) { return s.fs.Getwd() }
func (s *Snapshot) TempDir() string        { return s.fs.TempDir() }
func (s *Snapshot) Open(name string) (absfs.File, error) {
	if s.released {
		return nil, os.ErrClosed
	}

	dir, filename := s.fs.cleanPath(name)
	p := path.Join(dir, filename)

	node, err := s.resolve(p)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}

	if node.IsDir() {
		return &snapshotFile{
			snapshot: s,
			name:     name,
			node:     node,
			offset:   0,
			data:     nil, // Directories have no data
		}, nil
	}

	return &snapshotFile{
		snapshot: s,
		name:     name,
		node:     node,
		offset:   0,
	}, nil
}

// ensureData loads the whole file content lazily for non-chunked snapshots.
func (f *snapshotFile) ensureData() error {
	if f.loaded {
		return nil
	}
	data, _, err := f.snapshot.readContent(f.node)
	if err != nil {
		return err
	}
	f.data = data
	f.loaded = true
	return nil
}

// readRange reads part of a materialized snapshot file.
func (f *snapshotFile) readRange(off, n int64) ([]byte, error) {
	if n <= 0 {
		return nil, nil
	}
	if err, ok := f.snapshot.contentErr[f.node.Ino]; ok {
		return nil, err
	}
	data, ok := f.snapshot.content[f.node.Ino]
	if !ok {
		if f.snapshot.fs.contentFS != nil {
			full, err := f.snapshot.fs.loadData(f.node.Ino)
			if err != nil {
				return nil, err
			}
			data = full
		} else {
			return nil, nil
		}
	}
	if off >= int64(len(data)) {
		return nil, nil
	}
	end := off + n
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	out := make([]byte, end-off)
	copy(out, data[off:end])
	return out, nil
}
func (s *Snapshot) Create(name string) (absfs.File, error) { return nil, os.ErrPermission }
func (s *Snapshot) OpenFile(name string, flag int, perm os.FileMode) (absfs.File, error) {
	// Only allow read-only access for snapshots
	access := flag & absfs.O_ACCESS
	if access == os.O_RDONLY {
		return s.Open(name)
	}
	return nil, os.ErrPermission
}
func (s *Snapshot) Mkdir(name string, perm os.FileMode) error         { return os.ErrPermission }
func (s *Snapshot) MkdirAll(path string, perm os.FileMode) error      { return os.ErrPermission }
func (s *Snapshot) Remove(name string) error                          { return os.ErrPermission }
func (s *Snapshot) RemoveAll(path string) error                       { return os.ErrPermission }
func (s *Snapshot) Truncate(name string, size int64) error            { return os.ErrPermission }
func (s *Snapshot) Chmod(name string, mode os.FileMode) error         { return os.ErrPermission }
func (s *Snapshot) Chown(name string, uid, gid int) error             { return os.ErrPermission }
func (s *Snapshot) Chtimes(name string, atime, mtime time.Time) error { return os.ErrPermission }
func (s *Snapshot) Rename(oldpath, newpath string) error              { return os.ErrPermission }

// Sub returns an fs.FS corresponding to the subtree rooted at dir in the snapshot.
func (s *Snapshot) Sub(dir string) (fs.FS, error) {
	if s.released {
		return nil, os.ErrClosed
	}

	cleanDir, filename := s.fs.cleanPath(dir)
	p := path.Join(cleanDir, filename)
	node, err := s.resolve(p)
	if err != nil {
		return nil, &os.PathError{Op: "sub", Path: dir, Err: err}
	}

	if !node.IsDir() {
		return nil, &os.PathError{Op: "sub", Path: dir, Err: syscall.ENOTDIR}
	}

	// Return a subFS wrapper for the snapshot
	// snapshotSubFS already prefixes paths with p; see subFS.Sub.
	return absfs.FilerToFS(&snapshotSubFS{snapshot: s, dir: path.Clean(p)}, "/")
}

// snapshotSubFS wraps a Snapshot and prefixes all paths with a directory
type snapshotSubFS struct {
	snapshot *Snapshot
	dir      string
}

func (ss *snapshotSubFS) fullPath(name string) string {
	clean := path.Clean("/" + name)
	return path.Join(ss.dir, clean)
}

func (ss *snapshotSubFS) OpenFile(name string, flag int, perm os.FileMode) (absfs.File, error) {
	return ss.snapshot.OpenFile(ss.fullPath(name), flag, perm)
}

func (ss *snapshotSubFS) Mkdir(name string, perm os.FileMode) error {
	return os.ErrPermission
}

func (ss *snapshotSubFS) Remove(name string) error {
	return os.ErrPermission
}

func (ss *snapshotSubFS) Rename(oldpath, newpath string) error {
	return os.ErrPermission
}

func (ss *snapshotSubFS) Stat(name string) (os.FileInfo, error) {
	return ss.snapshot.Stat(ss.fullPath(name))
}

func (ss *snapshotSubFS) Chmod(name string, mode os.FileMode) error {
	return os.ErrPermission
}

func (ss *snapshotSubFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return os.ErrPermission
}

func (ss *snapshotSubFS) Chown(name string, uid, gid int) error {
	return os.ErrPermission
}

func (ss *snapshotSubFS) ReadDir(name string) ([]fs.DirEntry, error) {
	return ss.snapshot.ReadDir(ss.fullPath(name))
}

func (ss *snapshotSubFS) ReadFile(name string) ([]byte, error) {
	return ss.snapshot.ReadFile(ss.fullPath(name))
}

func (ss *snapshotSubFS) Sub(dir string) (fs.FS, error) {
	fullDir := ss.fullPath(dir)
	node, err := ss.snapshot.resolve(fullDir)
	if err != nil {
		return nil, &os.PathError{Op: "sub", Path: dir, Err: err}
	}
	if !node.IsDir() {
		return nil, &os.PathError{Op: "sub", Path: dir, Err: syscall.ENOTDIR}
	}
	return absfs.FilerToFS(&snapshotSubFS{snapshot: ss.snapshot, dir: path.Clean(fullDir)}, "/")
}

// snapshotFile implements absfs.File for read-only access to files in a snapshot.
type snapshotFile struct {
	snapshot  *Snapshot
	name      string
	node      *iNode
	offset    int64
	data      []byte
	loaded    bool
	diroffset int
}

// Ensure snapshotFile implements absfs.File
var _ absfs.File = (*snapshotFile)(nil)

func (f *snapshotFile) Name() string {
	return f.name
}

func (f *snapshotFile) Read(p []byte) (int, error) {
	if f.snapshot.released {
		return 0, os.ErrClosed
	}

	if f.node.IsDir() {
		return 0, &os.PathError{Op: "read", Path: f.name, Err: syscall.EISDIR}
	}

	if f.offset >= f.node.Size {
		return 0, io.EOF
	}

	if f.snapshot.fs.chunked() && f.snapshot.fs.contentFS == nil {
		want := int64(len(p))
		if f.offset+want > f.node.Size {
			want = f.node.Size - f.offset
		}
		data, err := f.readRange(f.offset, want)
		if err != nil {
			return 0, err
		}
		n := copy(p, data)
		f.offset += int64(n)
		if n < len(p) {
			return n, io.EOF
		}
		return n, nil
	}

	if err := f.ensureData(); err != nil {
		return 0, err
	}
	if f.offset >= int64(len(f.data)) {
		return 0, io.EOF
	}

	n := copy(p, f.data[f.offset:])
	f.offset += int64(n)
	return n, nil
}

func (f *snapshotFile) ReadAt(b []byte, off int64) (n int, err error) {
	if f.snapshot.released {
		return 0, os.ErrClosed
	}

	if f.node.IsDir() {
		return 0, &os.PathError{Op: "read", Path: f.name, Err: syscall.EISDIR}
	}

	if off < 0 {
		return 0, &os.PathError{Op: "readat", Path: f.name, Err: os.ErrInvalid}
	}

	if off >= f.node.Size {
		return 0, io.EOF
	}

	if f.snapshot.fs.chunked() && f.snapshot.fs.contentFS == nil {
		want := int64(len(b))
		if off+want > f.node.Size {
			want = f.node.Size - off
		}
		data, rerr := f.readRange(off, want)
		if rerr != nil {
			return 0, rerr
		}
		n = copy(b, data)
		if n < len(b) {
			err = io.EOF
		}
		return n, err
	}

	if err := f.ensureData(); err != nil {
		return 0, err
	}
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}

	n = copy(b, f.data[off:])
	if n < len(b) {
		err = io.EOF
	}
	return n, err
}

func (f *snapshotFile) Write(p []byte) (int, error) {
	return 0, &os.PathError{Op: "write", Path: f.name, Err: os.ErrPermission}
}

func (f *snapshotFile) WriteAt(b []byte, off int64) (n int, err error) {
	return 0, &os.PathError{Op: "writeat", Path: f.name, Err: os.ErrPermission}
}

func (f *snapshotFile) WriteString(s string) (n int, err error) {
	return 0, &os.PathError{Op: "write", Path: f.name, Err: os.ErrPermission}
}

func (f *snapshotFile) Seek(offset int64, whence int) (ret int64, err error) {
	if f.snapshot.released {
		return 0, os.ErrClosed
	}

	switch whence {
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	case io.SeekEnd:
		f.offset = f.node.Size + offset
	default:
		return 0, &os.PathError{Op: "seek", Path: f.name, Err: os.ErrInvalid}
	}

	if f.offset < 0 {
		f.offset = 0
		return 0, &os.PathError{Op: "seek", Path: f.name, Err: os.ErrInvalid}
	}

	return f.offset, nil
}

func (f *snapshotFile) Close() error {
	// Read-only file, nothing to sync or close
	return nil
}

func (f *snapshotFile) Sync() error {
	// Read-only file, nothing to sync
	return nil
}

func (f *snapshotFile) Stat() (os.FileInfo, error) {
	if f.snapshot.released {
		return nil, os.ErrClosed
	}
	return &fileinfo{name: path.Base(f.name), node: f.node}, nil
}

func (f *snapshotFile) Truncate(size int64) error {
	return &os.PathError{Op: "truncate", Path: f.name, Err: os.ErrPermission}
}

func (f *snapshotFile) Readdir(n int) ([]os.FileInfo, error) {
	if f.snapshot.released {
		return nil, os.ErrClosed
	}

	if !f.node.IsDir() {
		return nil, &os.PathError{Op: "readdir", Path: f.name, Err: syscall.ENOTDIR}
	}

	children := f.node.Children

	// Calculate how many entries to return
	remaining := len(children) - f.diroffset

	// When n <= 0, read all remaining entries and return nil error
	// (even if there are no entries left)
	if n <= 0 {
		if remaining <= 0 {
			return []os.FileInfo{}, nil
		}
		n = remaining
	} else {
		// When n > 0 and no entries remain, return io.EOF
		if remaining <= 0 {
			return nil, io.EOF
		}
		if n > remaining {
			n = remaining
		}
	}

	infos := make([]os.FileInfo, n)
	endOffset := f.diroffset + n
	for i, entry := range children[f.diroffset:endOffset] {
		node, err := f.snapshot.inode(entry.Ino)
		if err != nil {
			return nil, err
		}
		infos[i] = &fileinfo{name: entry.Name, node: node}
	}
	f.diroffset = endOffset
	return infos, nil
}

func (f *snapshotFile) ReadDir(n int) ([]fs.DirEntry, error) {
	if f.snapshot.released {
		return nil, os.ErrClosed
	}

	if !f.node.IsDir() {
		return nil, &os.PathError{Op: "readdir", Path: f.name, Err: syscall.ENOTDIR}
	}

	children := f.node.Children

	// Calculate how many entries to return
	remaining := len(children) - f.diroffset

	// When n <= 0, read all remaining entries and return nil error
	// (even if there are no entries left)
	if n <= 0 {
		if remaining <= 0 {
			return []fs.DirEntry{}, nil
		}
		n = remaining
	} else {
		// When n > 0 and no entries remain, return io.EOF
		if remaining <= 0 {
			return nil, io.EOF
		}
		if n > remaining {
			n = remaining
		}
	}

	entries := make([]fs.DirEntry, n)
	endOffset := f.diroffset + n
	for i, entry := range children[f.diroffset:endOffset] {
		node, err := f.snapshot.inode(entry.Ino)
		if err != nil {
			return nil, err
		}
		entries[i] = &dirEntry{name: entry.Name, node: node}
	}
	f.diroffset = endOffset
	return entries, nil
}

func (f *snapshotFile) Readdirnames(n int) ([]string, error) {
	if f.snapshot.released {
		return nil, os.ErrClosed
	}

	if !f.node.IsDir() {
		return nil, &os.PathError{Op: "readdirnames", Path: f.name, Err: syscall.ENOTDIR}
	}

	children := f.node.Children

	// Calculate how many entries to return
	remaining := len(children) - f.diroffset

	// When n <= 0, read all remaining entries and return nil error
	// (even if there are no entries left)
	if n <= 0 {
		if remaining <= 0 {
			return []string{}, nil
		}
		n = remaining
	} else {
		// When n > 0 and no entries remain, return io.EOF
		if remaining <= 0 {
			return nil, io.EOF
		}
		if n > remaining {
			n = remaining
		}
	}

	list := make([]string, n)
	endOffset := f.diroffset + n
	for i, entry := range children[f.diroffset:endOffset] {
		list[i] = entry.Name
	}
	f.diroffset = endOffset
	return list, nil
}
