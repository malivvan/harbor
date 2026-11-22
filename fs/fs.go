package fs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/storm/internal/absfs"
)

var errNilIno = errors.New("ino is nil")
var errNoData = errors.New("no data")

// FS implements absfs.FileSystem for the boltdb packages `github.com/coreos/bbolt`.
type FS struct {
	db        *bolt.DB
	bucket    string
	rootIno   uint64
	cwd       string
	cache     *inodeCache
	contentFS absfs.FileSystem // Optional external filesystem for storing file content

	// enc holds the encryption state. It is an atomic pointer so that key
	// rotation (Store.adoptEnc) can swap it while other goroutines are doing
	// file I/O without a data race. A nil value means encryption is disabled.
	enc atomic.Pointer[encState]

	// umask caches the persisted creation mask.
	umask atomic.Uint32

	// mu serializes inode load-modify-save cycles so concurrent operations on
	// the same directory or inode do not lose updates. Read paths take RLock so
	// a multi-transaction read cannot observe a concurrent mutation.
	mu sync.RWMutex
}

// encState returns the current encryption state (nil when disabled).
func (fs *FS) encState() *encState {
	return fs.enc.Load()
}

// newFS creates a new FS in the convention of other `absfs`
// implementations. It takes a bolt.DB pointer and a bucket path.
func newFS(db *bolt.DB, bucketpath string, enc *encState) (*FS, error) {

	// create buckets
	err := db.Update(func(tx *bolt.Tx) error {
		return bucketInit(tx, bucketpath)
	})
	if err != nil {
		return nil, err
	}

	// load or initialize
	rootIno := uint64(1)
	fs := &FS{
		db:        db,
		bucket:    bucketpath,
		rootIno:   rootIno,
		cwd:       "/",
		cache:     newInodeCache(1000), // Default cache size of 1000 inodes
		contentFS: nil,                 // Use BoltDB by default (backward compatible)
	}
	fs.enc.Store(enc)
	err = db.Update(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, bucketpath)
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		b.enc = enc
		if enc != nil && b.state.Get([]byte(markerKey)) == nil {
			raw, err := enc.encodeMarker()
			if err != nil {
				return err
			}
			if err := b.state.Put([]byte(markerKey), raw); err != nil {
				return err
			}
			{
				// An otherwise empty plaintext filesystem may contain the two
				// system inodes; re-encode them with the new encryption state.
				for ino := uint64(0); ino <= 1; ino++ {
					node := new(iNode)
					err := b.decodeRawNode(ino, node)
					if errors.Is(err, os.ErrNotExist) {
						continue
					}
					if err != nil {
						return err
					}
					if err := b.encodeNode(ino, node); err != nil {
						return err
					}
				}
			}
		}

		// create the `nil` node if it doesn't exist
		err = b.InodeInit()
		if err != nil {
			return err
		}

		// load the
		data := make([]byte, 4)
		binary.BigEndian.PutUint32(data, uint32(0022))
		umaskData, err := b.LoadOrSet("umask", data)
		if err != nil {
			return err
		}
		fs.umask.Store(binary.BigEndian.Uint32(umaskData))

		// load the root Ino if one is available
		data, err = b.LoadOrSet("rootIno", i2b(rootIno))
		if err != nil {
			return err
		}
		rootIno = b2i(data)

		_, err = b.GetInode(rootIno)
		if err == nil {
			return nil
		}

		if err == os.ErrNotExist {
			node := newInode(os.ModeDir | 0755)
			node.countUp()
			err = b.PutInode(rootIno, node)
			if err != nil {

				return err
			}
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	fs.rootIno = rootIno
	return fs, nil

}

// Close releases the file system wrapper. The underlying bbolt database is
// owned by the Store and is not closed.
func (fs *FS) Close() error {
	return nil
}

// SetContentFS sets an external filesystem for storing file content.
// This allows file content to be stored in any absfs.FileSystem implementation
// (memfs, osfs, s3fs, etc.) instead of in BoltDB. This should be called before
// any file operations.
func (fs *FS) SetContentFS(contentFS absfs.FileSystem) {
	fs.contentFS = contentFS
}

// openFsBucket opens the filesystem buckets at the configured bucket path.
// This ensures proper bucket isolation when a non-empty bucket path is configured.
func (fs *FS) openFsBucket(tx *bolt.Tx) (*fsBucket, error) {
	bb, err := openBucket(tx, fs.bucket)
	if err != nil {
		return nil, err
	}
	b := newFsBucket(bb)
	b.enc = fs.enc.Load()
	return b, nil
}

// openFsBucketWithCache opens the filesystem buckets with cache support.
// This ensures proper bucket isolation when a non-empty bucket path is configured.
func (fs *FS) openFsBucketWithCache(tx *bolt.Tx) (*fsBucket, error) {
	bb, err := openBucket(tx, fs.bucket)
	if err != nil {
		return nil, err
	}
	b := newFsBucketWithCache(bb, fs.cache)
	b.enc = fs.enc.Load()
	return b, nil
}

// CacheStats returns statistics about the inode cache.
func (fs *FS) CacheStats() CacheStats {
	if fs.cache == nil {
		return CacheStats{}
	}
	return fs.cache.Stats()
}

// FlushCache removes all entries from the inode cache.
func (fs *FS) FlushCache() {
	if fs.cache != nil {
		fs.cache.Flush()
	}
}

// SetCacheSize changes the maximum size of the inode cache.
// Setting size to 0 or negative disables the cache.
func (fs *FS) SetCacheSize(size int) {
	if fs.cache != nil {
		fs.cache.Enable(size)
	}
}

// Umask returns the current `umask` value. The mask is applied to the
// permission bits requested for new files and directories. Returns 0022 if an
// error occurs.
func (fs *FS) Umask() os.FileMode {
	if fs == nil {
		return 0022
	}
	umask := os.FileMode(fs.umask.Load())
	if umask == 0 {
		return 0022
	}
	return umask
}

// SetUmask sets the current `umask` value. Silently ignores errors.
func (fs *FS) SetUmask(umask os.FileMode) {
	var data [4]byte

	err := fs.db.Update(func(tx *bolt.Tx) error {
		b, err := fs.openFsBucket(tx)
		if err != nil {
			return err
		}

		binary.BigEndian.PutUint32(data[:], uint32(umask))
		return b.Put("umask", data[:])
	})
	if err != nil {
		// Silently ignore errors to maintain backwards compatibility.
		return
	}
	fs.umask.Store(uint32(umask))
}

// TempDir returns the path to a temporary directory. Returns "/tmp" if an error occurs.
func (fs *FS) TempDir() string {
	var tempdir string
	tempdir = "/tmp"
	err := fs.db.Update(func(tx *bolt.Tx) error {
		b, err := fs.openFsBucket(tx)
		if err != nil {
			return err
		}
		data, err := b.Get("tempdir")
		if err == nil && data != nil {
			tempdir = string(data)
			return nil
		}
		return b.Put("tempdir", []byte(tempdir))
	})
	if err != nil {
		// Return default temp directory on error instead of panicking
		return "/tmp"
	}

	return tempdir
}

// SetTempdir sets the path to a temporary directory, but does not create the
// actual directories. Silently ignores errors.
func (fs *FS) SetTempdir(tempdir string) {
	// Ignore the error to maintain backwards compatibility
	_ = fs.db.Update(func(tx *bolt.Tx) error {
		b, err := fs.openFsBucket(tx)
		if err != nil {
			return err
		}
		return b.Put("tempdir", []byte(tempdir))
	})
}

// saveInode save an iNode to the databased.  If the iNode's `ino` number is
// non-zero the node will be saved with the `ino` provided.
// If `ino` is zero (the nil value) then a new `ino` is created. In both
// cases the `ino` value is returned.
func (fs *FS) saveInode(node *iNode) (ino uint64, err error) {
	ino = node.Ino
	err = fs.db.Update(func(tx *bolt.Tx) error {
		b, err := fs.openFsBucketWithCache(tx)
		if err != nil {
			return err
		}

		if ino == 0 {
			ino, err = b.NextInode()
		}
		if err != nil {
			return err
		}
		return b.PutInode(ino, node)

	})
	return ino, err
}

// ReadDir reads the named directory and returns a list of directory entries sorted by filename.
func (filesystem *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	filesystem.mu.RLock()
	defer filesystem.mu.RUnlock()
	return filesystem.readDir(name)
}

// readDir is ReadDir without the lock; callers must hold fs.mu.
func (filesystem *FS) readDir(name string) ([]fs.DirEntry, error) {
	node, err := filesystem.resolveFollow(name, 0)
	if err != nil {
		return nil, &os.PathError{Op: "readdir", Path: name, Err: err}
	}

	if !node.IsDir() {
		return nil, &os.PathError{Op: "readdir", Path: name, Err: syscall.ENOTDIR}
	}

	var entries []fs.DirEntry
	for _, child := range node.Children {
		childNode, err := filesystem.loadInode(child.Ino)
		if err != nil {
			return nil, &os.PathError{Op: "readdir", Path: name, Err: err}
		}
		entries = append(entries, &dirEntry{
			name: child.Name,
			node: childNode,
		})
	}

	return entries, nil
}

// ReadFile reads the named file and returns its contents.
func (fs *FS) ReadFile(name string) ([]byte, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	node, err := fs.resolveFollow(name, 0)
	if err != nil {
		return nil, &os.PathError{Op: "readfile", Path: name, Err: err}
	}

	if node.IsDir() {
		return nil, &os.PathError{Op: "readfile", Path: name, Err: syscall.EISDIR}
	}

	var data []byte
	if fs.chunked() && fs.contentFS == nil {
		data, err = fs.readRange(node.Ino, 0, node.Size)
	} else {
		data, err = fs.loadData(node.Ino)
	}
	if err != nil {
		return nil, &os.PathError{Op: "readfile", Path: name, Err: err}
	}

	return data, nil
}

// Sub returns an fs.FS corresponding to the subtree rooted at dir.
func (fs *FS) Sub(dir string) (fs.FS, error) {
	cleanDir, filename := fs.cleanPath(dir)
	p := path.Join(cleanDir, filename)
	node, err := fs.resolve(p)
	if err != nil {
		return nil, &os.PathError{Op: "sub", Path: dir, Err: err}
	}

	if !node.IsDir() {
		return nil, &os.PathError{Op: "sub", Path: dir, Err: errors.New("not a directory")}
	}

	// Pass "/" as root since subFS is already rooted at the subdirectory
	return absfs.FilerToFS(&subFS{fs: fs, dir: path.Clean(p)}, "/")
}

// dirEntry implements fs.DirEntry
type dirEntry struct {
	name string
	node *iNode
}

func (d *dirEntry) Name() string {
	return d.name
}

func (d *dirEntry) IsDir() bool {
	return d.node.IsDir()
}

func (d *dirEntry) Type() fs.FileMode {
	return d.node.Mode.Type()
}

func (d *dirEntry) Info() (fs.FileInfo, error) {
	return &inodeinfo{d.name, d.node}, nil
}

// subFS wraps a FS and prefixes all paths with a directory
type subFS struct {
	fs  *FS
	dir string
}

func (s *subFS) fullPath(name string) string {
	// Prevent escaping the subtree
	clean := path.Clean("/" + name)
	return path.Join(s.dir, clean)
}

func (s *subFS) OpenFile(name string, flag int, perm os.FileMode) (absfs.File, error) {
	return s.fs.OpenFile(s.fullPath(name), flag, perm)
}

func (s *subFS) Mkdir(name string, perm os.FileMode) error {
	return s.fs.Mkdir(s.fullPath(name), perm)
}

func (s *subFS) Remove(name string) error {
	return s.fs.Remove(s.fullPath(name))
}

func (s *subFS) Rename(oldpath, newpath string) error {
	return s.fs.Rename(s.fullPath(oldpath), s.fullPath(newpath))
}

func (s *subFS) Stat(name string) (os.FileInfo, error) {
	return s.fs.Stat(s.fullPath(name))
}

func (s *subFS) Chmod(name string, mode os.FileMode) error {
	return s.fs.Chmod(s.fullPath(name), mode)
}

func (s *subFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return s.fs.Chtimes(s.fullPath(name), atime, mtime)
}

func (s *subFS) Chown(name string, uid, gid int) error {
	return s.fs.Chown(s.fullPath(name), uid, gid)
}

func (s *subFS) ReadDir(name string) ([]fs.DirEntry, error) {
	return s.fs.ReadDir(s.fullPath(name))
}

func (s *subFS) ReadFile(name string) ([]byte, error) {
	return s.fs.ReadFile(s.fullPath(name))
}

func (s *subFS) Sub(dir string) (fs.FS, error) {
	fullDir := s.fullPath(dir)
	node, err := s.fs.resolve(fullDir)
	if err != nil {
		return nil, &os.PathError{Op: "sub", Path: dir, Err: err}
	}
	if !node.IsDir() {
		return nil, &os.PathError{Op: "sub", Path: dir, Err: errors.New("not a directory")}
	}
	return absfs.FilerToFS(&subFS{fs: s.fs, dir: path.Clean(fullDir)}, dir)
}

var errInvalidIno = errors.New("invalid ino")

// inoToPath converts an inode number to a path in the content filesystem.
// Uses subdirectories (like Git) to avoid too many files in one directory.
// Format: /XX/XXXXXXXXXXXXXXXX where XX is the first 2 hex digits.
func inoToPath(ino uint64) string {
	hex := fmt.Sprintf("%016x", ino)
	// Create subdirectory structure: /01/0123456789abcdef
	return path.Join("/", hex[:2], hex)
}

// loadInode - loads the iNode defined by `ino` or returns an error
func (fs *FS) loadInode(ino uint64) (*iNode, error) {
	if ino == 0 {
		return nil, errNilIno
	}

	node := new(iNode)
	err := fs.db.View(func(tx *bolt.Tx) error {
		b, err := fs.openFsBucketWithCache(tx)
		if err != nil {
			return err
		}
		return b.decodeNode(ino, node)
	})

	return node, err
}

// saveData - saves file data for a given `ino` or returns an error
func (fs *FS) saveData(ino uint64, data []byte) error {
	if ino == 0 {
		return errNilIno
	}

	// Use external content filesystem if available, otherwise fall back to BoltDB data bucket
	if fs.contentFS != nil {
		contentPath := inoToPath(ino)

		// Ensure parent directory exists
		dir := path.Dir(contentPath)
		if err := fs.contentFS.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}

		f, err := fs.contentFS.OpenFile(contentPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return fmt.Errorf("failed to create file: %w", err)
		}

		sealed, err := fs.enc.Load().sealContent(data)
		if err == nil {
			_, err = f.Write(sealed)
		}
		// The external filesystem may flush/upload on Close (s3fs does), so the
		// Close error is part of the write result.
		closeErr := f.Close()
		if err != nil {
			return fmt.Errorf("failed to write data: %w", err)
		}
		if closeErr != nil {
			return fmt.Errorf("failed to finalize data: %w", closeErr)
		}
		return nil
	}

	if fs.chunked() {
		return fs.saveChunkedData(ino, data)
	}

	sealed, err := fs.enc.Load().sealContent(data)
	if err != nil {
		return err
	}

	return fs.db.Update(func(tx *bolt.Tx) error {
		b, err := fs.openFsBucket(tx)
		if err != nil {
			return err
		}
		return b.data.Put(i2b(ino), sealed)
	})
}

// loadData - loads file data for a given `ino` or returns an error
func (fs *FS) loadData(ino uint64) ([]byte, error) {
	if ino == 0 {
		return nil, errNilIno
	}

	// Use external content filesystem if available, otherwise fall back to BoltDB data bucket
	if fs.contentFS != nil {
		contentPath := inoToPath(ino)

		f, err := fs.contentFS.OpenFile(contentPath, os.O_RDONLY, 0)
		if err != nil {
			if os.IsNotExist(err) {
				return []byte{}, nil
			}
			return nil, fmt.Errorf("failed to open file: %w", err)
		}
		defer f.Close()

		data, err := io.ReadAll(f)
		if err != nil {
			return nil, fmt.Errorf("failed to read file: %w", err)
		}

		plain, err := fs.enc.Load().openContent(data)
		if err != nil {
			return nil, err
		}
		return plain, nil
	}

	if fs.chunked() {
		return fs.loadChunkedData(ino)
	}

	var data []byte
	err := fs.db.View(func(tx *bolt.Tx) error {
		b, err := fs.openFsBucket(tx)
		if err != nil {
			return err
		}
		d := b.data.Get(i2b(ino))
		if d == nil {
			data = []byte{}
			return nil
		}
		plain, err := fs.enc.Load().openContent(d)
		if err != nil {
			return err
		}
		data = make([]byte, len(plain))
		copy(data, plain)
		return nil
	})
	if err != nil {
		return data, err
	}

	return data, err
}

// resizeBytes returns data resized to size, zero-padding when growing.
func resizeBytes(data []byte, size int64) []byte {
	if int64(len(data)) == size {
		return data
	}
	if int64(len(data)) > size {
		return data[:size]
	}
	out := make([]byte, size)
	copy(out, data)
	return out
}

// resizeContent resizes the content of node to size, handling external
// contentFS storage, chunked storage and encrypted whole-blob storage. It
// updates node.Size and persists the inode. size must be non-negative.
func (fs *FS) resizeContent(node *iNode, size int64) error {
	if size < 0 {
		return syscall.EINVAL
	}
	if size > maxFileSize {
		return syscall.EFBIG
	}
	ino := node.Ino

	switch {
	case fs.contentFS != nil:
		data, err := fs.loadData(ino)
		if err != nil {
			return err
		}
		if err := fs.saveData(ino, resizeBytes(data, size)); err != nil {
			return err
		}
	case fs.chunked():
		if err := fs.truncateRange(ino, size); err != nil {
			return err
		}
	default:
		data, err := fs.loadData(ino)
		if err != nil {
			return err
		}
		if err := fs.saveData(ino, resizeBytes(data, size)); err != nil {
			return err
		}
	}

	if node.Size != size {
		node.Size = size
		if _, err := fs.saveInode(node); err != nil {
			return err
		}
	}
	return nil
}

// isDescendantPath reports whether child is parent itself or lives inside it.
// Both paths must be clean, absolute virtual paths.
func isDescendantPath(parent, child string) bool {
	parent = path.Clean(parent)
	child = path.Clean(child)
	if parent == child {
		return true
	}
	if parent == "/" {
		return strings.HasPrefix(child, "/")
	}
	return strings.HasPrefix(child, parent+"/")
}

// cleanPath - takes the absolute or relative path provided by `name` and
// returns the directory and filename of the cleand absolute path.
func (fs *FS) cleanPath(name string) (string, string) {
	p := name
	if !path.IsAbs(p) {
		p = path.Join(fs.cwd, p)
	}
	dir, filename := path.Split(p)
	dir = path.Clean(dir)
	return dir, filename
}

// Rename renames (moves) oldpath to newpath. If newpath already exists and
// is not a directory, Rename replaces it. OS-specific restrictions may apply
// when oldpath and newpath are in different directories. If there is an
// error, it will be of type *LinkError.
func (fs *FS) Rename(oldpath, newpath string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	linkErr := &os.LinkError{Op: "move", Old: oldpath, New: newpath}
	if oldpath == "/" {
		linkErr.Err = errors.New("the root folder may not be moved or renamed")
		return linkErr
	}

	srcDir, srcFilename := fs.cleanPath(oldpath)
	dstDir, dstFilename := fs.cleanPath(newpath)
	srcParent, srcChild := fs.loadParentChild(srcDir, srcFilename)
	dstParent, dstChild := fs.loadParentChild(dstDir, dstFilename)

	if srcParent == nil {
		linkErr.Err = os.ErrNotExist
		linkErr.Old = srcDir
		return linkErr
	}
	if srcChild == nil {
		linkErr.Err = os.ErrNotExist
		linkErr.Old = path.Join(srcDir, srcFilename)
		return linkErr
	}
	if dstParent == nil {
		linkErr.Err = os.ErrNotExist
		linkErr.New = dstDir
		return linkErr
	}
	sameDir := srcParent.Ino == dstParent.Ino
	if sameDir && srcFilename == dstFilename {
		// Reject an identity rename explicitly: reporting success would hide
		// a caller that resolved the wrong destination.
		linkErr.Err = syscall.EINVAL
		return linkErr
	}

	// Moving a directory into itself or one of its own descendants would
	// detach the whole subtree from the tree.
	if dstParent.Ino == srcChild.Ino || isDescendantPath(path.Join(srcDir, srcFilename), dstDir) {
		linkErr.Err = syscall.EINVAL
		return linkErr
	}

	// POSIX lets a non-directory replace a non-directory. Replacing a
	// directory is only possible for an empty one, which this implementation
	// does not attempt.
	if dstChild != nil && (srcChild.IsDir() || dstChild.IsDir()) {
		if dstChild.IsDir() && !srcChild.IsDir() {
			linkErr.Err = syscall.EISDIR
		} else {
			linkErr.Err = syscall.EEXIST
		}
		linkErr.New = path.Join(dstDir, dstFilename)
		return linkErr
	}

	// Remove the replaced entry before linking the new name.
	if dstChild != nil {
		if _, err := dstParent.Unlink(dstFilename); err != nil {
			linkErr.Err = err
			return linkErr
		}
	}

	if _, err := dstParent.Link(dstFilename, srcChild.Ino); err != nil {
		linkErr.Err = err
		return linkErr
	}

	if sameDir {
		// The source and destination names share one parent inode.
		if _, err := dstParent.Unlink(srcFilename); err != nil {
			linkErr.Err = err
			return linkErr
		}
	} else {
		if _, err := srcParent.Unlink(srcFilename); err != nil {
			linkErr.Err = err
			return linkErr
		}
	}

	// Persist both parents, and the replaced inode, in one transaction. A
	// failure can therefore never leave two names for one inode without a
	// matching link count.
	deleteReplaced := false
	err := fs.db.Update(func(tx *bolt.Tx) error {
		b, err := fs.openFsBucketWithCache(tx)
		if err != nil {
			return err
		}
		if err := b.PutInode(dstParent.Ino, dstParent); err != nil {
			return err
		}
		if !sameDir {
			if err := b.PutInode(srcParent.Ino, srcParent); err != nil {
				return err
			}
		}
		if dstChild == nil {
			return nil
		}
		if dstChild.countDown() == 0 {
			deleteReplaced = true
			key := i2b(dstChild.Ino)
			if err := b.inodes.Delete(key); err != nil {
				return err
			}
			if fs.contentFS == nil {
				b.data.Delete(key)
			}
			b.symlinks.Delete(key)
			return b.deleteAllChunks(dstChild.Ino)
		}
		return b.PutInode(dstChild.Ino, dstChild)
	})
	if err != nil {
		linkErr.Err = err
		return linkErr
	}
	if deleteReplaced && fs.contentFS != nil {
		if err := fs.contentFS.Remove(inoToPath(dstChild.Ino)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	return nil
}

// Copy is a convenience function that duplicates the `source` path to the
// `newpath`
func (fs *FS) Copy(source, destination string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	pathErr := &os.PathError{Op: "copy", Path: source}
	if source == "/" {
		pathErr.Err = errors.New("the root folder may not be moved or renamed")
		return pathErr
	}

	srcDir, srcFilename := fs.cleanPath(source)
	dstDir, dstFilename := fs.cleanPath(destination)
	_, srcChild := fs.loadParentChild(srcDir, srcFilename)
	dstParent, dstChild := fs.loadParentChild(dstDir, dstFilename)

	if srcChild == nil {
		pathErr.Err = os.ErrNotExist
		pathErr.Path = path.Join(srcDir, srcFilename)
		return pathErr
	}
	if dstParent == nil {
		pathErr.Err = os.ErrNotExist
		pathErr.Path = dstDir
		return pathErr
	}
	if dstChild != nil {
		pathErr.Err = os.ErrExist
		pathErr.Path = path.Join(dstDir, dstFilename)
		return pathErr
	}

	// Copying a directory into itself would create a self-referential tree.
	if dstParent.Ino == srcChild.Ino || isDescendantPath(path.Join(srcDir, srcFilename), dstDir) {
		pathErr.Err = syscall.EINVAL
		return pathErr
	}

	ino, err := fs.copyNode(srcChild)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}

	_, err = dstParent.Link(dstFilename, ino)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}

	_, err = fs.saveInode(dstParent)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}

	return nil
}

// copyNode deep-copies src into a newly allocated inode, returning the new
// inode number. It copies the file content (data bucket, chunks, external
// contentFS file) and symlink targets, and recursively copies directory
// children into brand-new inodes so the copy is fully independent of the
// source.
func (fs *FS) copyNode(src *iNode) (uint64, error) {
	node := copyInode(src)
	node.Ino = 0
	node.Nlink = 1
	if src.IsDir() {
		node.Children = nil
	}

	newIno, err := fs.saveInode(node)
	if err != nil {
		return 0, err
	}
	copied := false
	defer func() {
		if !copied {
			_ = fs.deleteInode(newIno)
		}
	}()

	switch {
	case src.Mode&os.ModeSymlink != 0:
		link, err := fs.readLink(src.Ino)
		if err != nil {
			return 0, err
		}
		err = fs.db.Update(func(tx *bolt.Tx) error {
			b, err := fs.openFsBucket(tx)
			if err != nil {
				return err
			}
			return b.Symlink(newIno, link)
		})
		if err != nil {
			return 0, err
		}
	case src.IsDir():
		for _, child := range src.Children {
			childNode, err := fs.loadInode(child.Ino)
			if err != nil {
				return 0, err
			}
			childIno, err := fs.copyNode(childNode)
			if err != nil {
				return 0, err
			}
			if _, err := node.Link(child.Name, childIno); err != nil {
				return 0, err
			}
		}
		if _, err := fs.saveInode(node); err != nil {
			return 0, err
		}
	default:
		data, err := fs.loadData(src.Ino)
		if err != nil {
			return 0, err
		}
		if err := fs.saveData(newIno, data); err != nil {
			return 0, err
		}
	}

	copied = true
	return newIno, nil
}

// readLink returns the symlink target stored for ino.
func (fs *FS) readLink(ino uint64) (string, error) {
	var link string
	err := fs.db.View(func(tx *bolt.Tx) error {
		b, err := fs.openFsBucket(tx)
		if err != nil {
			return err
		}
		link = b.Readlink(ino)
		return nil
	})
	return link, err
}

// Chdir - changes the current directory to the absolute or relative path
// provided by `Chdir`
func (fs *FS) Chdir(name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	dir, filename := fs.cleanPath(name)
	node, err := fs.resolveFollow(path.Join(dir, filename), 0)
	if err != nil {
		return err
	}
	if !node.IsDir() {
		return &os.PathError{Op: "chdir", Path: name, Err: syscall.ENOTDIR}
	}
	fs.cwd = path.Join(dir, filename)
	return nil
}

// Getwd returns the current working directory, the error value is always `nil`.
func (fs *FS) Getwd() (dir string, err error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.cwd, nil
}

// Open is a convenience function that opens a file in read only mode.
func (fs *FS) Open(name string) (absfs.File, error) {
	return fs.OpenFile(name, os.O_RDONLY, 0)
}

// Create is a convenience function that opens a file for reading and writing.
// If the file does not exist it is created, if it does then it is truncated.
func (fs *FS) Create(name string) (absfs.File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.openFile(name, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0777, 0)
}

// resolveFollow resolves name and follows a final symlink (with a depth bound),
// which is what ReadFile/ReadDir/Truncate/Chmod/Chtimes need.
func (fs *FS) resolveFollow(name string, depth int) (*iNode, error) {
	if depth > maxSymlinkDepth {
		return nil, syscall.ELOOP
	}
	dir, filename := fs.cleanPath(name)
	node, err := fs.resolve(path.Join(dir, filename))
	if err != nil {
		return nil, err
	}
	if node.Mode&os.ModeSymlink == 0 {
		return node, nil
	}
	link, err := fs.readLink(node.Ino)
	if err != nil {
		return nil, err
	}
	if !path.IsAbs(link) {
		link = path.Join(path.Dir(name), link)
	}
	return fs.resolveFollow(link, depth+1)
}

// resolve resolves the path provided into a iNode, or an error
func (fs *FS) resolve(path string) (*iNode, error) {
	node := new(iNode)

	err := fs.db.View(func(tx *bolt.Tx) error {
		b, err := fs.openFsBucketWithCache(tx)
		if err != nil {
			return err
		}

		ino := fs.rootIno
		loadedNode, err := b.GetInode(ino)
		if err != nil {
			return err
		}
		*node = *loadedNode

		if path == "/" {
			return nil
		}

		for _, name := range strings.Split(strings.TrimLeft(path, "/"), "/") {
			// find the child's ino or error
			x := sort.Search(len(node.Children), func(i int) bool {
				return node.Children[i].Name >= name
			})
			if x == len(node.Children) || node.Children[x].Name != name {
				return os.ErrNotExist
			}

			// replace node with child or error
			loadedNode, err = b.GetInode(node.Children[x].Ino)
			if err != nil {
				return err
			}
			*node = *loadedNode
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return node, nil

}

const (
	O_RDONLY = 0       // open the file read-only.
	O_WRONLY = 1       // open the file write-only.
	O_RDWR   = 2       // open the file read-write.
	O_APPEND = 1024    // append data to the file when writing.
	O_CREATE = 64      // create a new file if none exists.
	O_EXCL   = 128     // used with O_CREATE, file must not exist.
	O_SYNC   = 1052672 // open for synchronous I/O.
	O_TRUNC  = 512     // truncate regular writable file when opened.
)

// OpenFile is the generalized open call; most users will use Open or Create
// instead. It opens the named file with specified flag (O_RDONLY etc.) and
// perm (before umask), if applicable. If successful, methods on the returned
// File can be used for I/O. If there is an error, it will be of type
// *os.PathError.
func (fs *FS) OpenFile(name string, flag int, perm os.FileMode) (absfs.File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.openFile(name, flag, perm, 0)
}

// openFile is the implementation of OpenFile. depth tracks how many symlinks
// have been followed so cyclic links cannot recurse without bound.
func (fs *FS) openFile(name string, flag int, perm os.FileMode, depth int) (absfs.File, error) {
	file := &absfs.InvalidFile{Path: name}
	pathErr := &os.PathError{Op: "open", Path: name}

	if depth > maxSymlinkDepth {
		pathErr.Err = syscall.ELOOP
		return file, pathErr
	}

	dir, filename := fs.cleanPath(name)
	parent, child := fs.loadParentChild(dir, filename)
	if parent == nil {
		pathErr.Err = os.ErrNotExist
		pathErr.Path = dir
		return file, pathErr
	}

	access := flag & absfs.O_ACCESS
	if dir == "/" && filename == "" {
		child = parent
	}

	if child == nil {
		// error if it does not exist, and we are not allowed to create it.
		if flag&os.O_CREATE == 0 {
			pathErr.Err = syscall.ENOENT
			return file, pathErr
		}

		// Create file, applying the configured umask to the permission bits.
		child = newInode(perm &^ os.ModeType &^ fs.Umask())
		err := fs.saveParentChild(parent, filename, child)
		if err != nil {
			pathErr.Err = err
			return file, pathErr
		}
	} else { // child exists
		if flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0 {
			pathErr.Err = syscall.EEXIST
			return file, pathErr
		}

		// Follow symlinks
		if child.Mode&os.ModeSymlink != 0 {
			link, err := fs.readLink(child.Ino)
			if err != nil {
				pathErr.Err = err
				return file, pathErr
			}
			if !path.IsAbs(link) {
				// Relative symlinks are relative to the directory containing the symlink
				link = path.Join(dir, link)
			}
			return fs.openFile(link, flag, perm, depth+1)
		}

		if child.Mode.IsDir() {
			if access != os.O_RDONLY || flag&os.O_TRUNC != 0 {
				pathErr.Err = syscall.EISDIR
				return file, pathErr
			}
		}

		// if we must truncate the file
		if flag&os.O_TRUNC != 0 {
			if err := fs.resizeContent(child, 0); err != nil {
				pathErr.Err = err
				return file, pathErr
			}
		}
	}

	if flag&os.O_CREATE == 0 {
		if access == os.O_RDONLY && child.Mode&absfs.OS_ALL_R == 0 ||
			access == os.O_WRONLY && child.Mode&absfs.OS_ALL_W == 0 ||
			access == os.O_RDWR && child.Mode&(absfs.OS_ALL_W|absfs.OS_ALL_R) == 0 {
			pathErr.Err = os.ErrPermission
			return file, pathErr
		}
	}

	f := &File{fs: fs, name: name, flags: flag, node: child}
	if flag&os.O_APPEND != 0 {
		f.offset = child.Size
	}
	return f, nil
}

// Stat returns the FileInfo structure describing file. If there is an error,
// it will be of type *os.PathError.
func (fs *FS) Stat(name string) (os.FileInfo, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.statWithDepth(name, 0)
}

// statWithDepth is the internal implementation of Stat with symlink cycle detection.
// maxSymlinkDepth limits the number of symlinks that can be followed.
const maxSymlinkDepth = 40

func (fs *FS) statWithDepth(name string, depth int) (os.FileInfo, error) {
	if depth > maxSymlinkDepth {
		return nil, &os.PathError{Op: "stat", Path: name, Err: syscall.ELOOP}
	}

	dir, filename := fs.cleanPath(name)
	node, err := fs.resolve(path.Join(dir, filename))
	if err != nil {
		return nil, err
	}

	if node.Mode&os.ModeSymlink == 0 {
		if filename == "" {
			filename = dir
		}
		return inodeinfo{filename, node}, nil
	}

	link, err := fs.readLink(node.Ino)
	if err != nil {
		return nil, err
	}

	if !path.IsAbs(link) {
		// Relative symlinks are relative to the directory containing the symlink
		link = path.Join(path.Dir(name), link)
	}

	return fs.statWithDepth(link, depth+1)
}

// Truncate changes the size of the file. It does not change the I/O offset. If
// there is an error, it will be of type *os.PathError.
func (fs *FS) Truncate(name string, size int64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if size < 0 {
		return &os.PathError{Op: "truncate", Path: name, Err: syscall.EINVAL}
	}

	dir, filename := fs.cleanPath(name)
	node, err := fs.resolveFollow(path.Join(dir, filename), 0)
	if err != nil {
		return err
	}

	return fs.resizeContent(node, size)
}

// loadParentChild loads the node for `dir` and the child nodes with name
// `filename` or nil.
func (fs *FS) loadParentChild(dir, filename string) (*iNode, *iNode) {
	if fs == nil {
		panic("receiver may not be nil")
	}
	filename = strings.Trim(filename, "/")

	if dir == "/" && filename == "" {
		node, err := fs.loadInode(fs.rootIno)
		if err != nil {
			return nil, nil
		}
		return node, nil
	}

	parent, err := fs.resolve(dir)
	if err != nil {
		return nil, nil
	}

	if !sort.IsSorted(parent.Children) {
		sort.Sort(parent.Children)
	}

	i := sort.Search(len(parent.Children), func(i int) bool {
		return parent.Children[i].Name >= filename
	})
	if i < len(parent.Children) && parent.Children[i].Name == filename {
		// found
		child, err := fs.loadInode(parent.Children[i].Ino)
		if err != nil {
			return parent, nil
		}
		return parent, child
	}

	// not found
	return parent, nil
}

func (fs *FS) saveParentChild(parent *iNode, filename string, child *iNode) error {
	filename = strings.Trim(filename, "/")
	child.countUp()
	return fs.db.Update(func(tx *bolt.Tx) error {
		b, err := fs.openFsBucketWithCache(tx)
		if err != nil {
			return err
		}
		ino := child.Ino
		if ino == 0 {
			ino, err = b.NextInode()
			if err != nil {
				return err
			}
		}
		if err := b.PutInode(ino, child); err != nil {
			return err
		}
		old, err := parent.Link(filename, ino)
		if err != nil {
			return err
		}
		if old != 0 {
			oldChild, err := b.GetInode(old)
			if err != nil {
				return err
			}
			if oldChild.countDown() == 0 {
				key := i2b(oldChild.Ino)
				if err := b.inodes.Delete(key); err != nil {
					return err
				}
				if fs.contentFS == nil {
					b.data.Delete(key)
				}
				b.symlinks.Delete(key)
				if err := b.deleteAllChunks(oldChild.Ino); err != nil {
					return err
				}
			} else if err := b.PutInode(old, oldChild); err != nil {
				return err
			}
		}
		return b.PutInode(parent.Ino, parent)
	})
}

func (fs *FS) deleteInode(ino uint64) error {
	// Delete from external content filesystem first
	if fs.contentFS != nil {
		contentPath := inoToPath(ino)
		if err := fs.contentFS.Remove(contentPath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	err := fs.db.Update(func(tx *bolt.Tx) error {
		b, err := fs.openFsBucket(tx)
		if err != nil {
			return err
		}
		key := i2b(ino)
		err = b.inodes.Delete(key)
		if err != nil {
			return err
		}
		// Only delete from data bucket if no external content filesystem is configured
		if fs.contentFS == nil {
			b.data.Delete(key)
		}
		// Remove any chunked content and symlink target so they do not leak.
		b.symlinks.Delete(key)
		if err := b.deleteAllChunks(ino); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// Mkdir creates a new directory with the specified name and permission bits (before umask).
// If there is an error, it will be of type *PathError.
func (fs *FS) Mkdir(name string, perm os.FileMode) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.mkdir(name, perm)
}

func (fs *FS) mkdir(name string, perm os.FileMode) error {
	pathErr := &os.PathError{Op: "mkdir", Path: name}
	dir, filename := fs.cleanPath(name)
	parent, child := fs.loadParentChild(dir, filename)
	if parent == nil {
		pathErr.Err = os.ErrNotExist
		pathErr.Path = dir
		return pathErr
	}
	if child != nil {
		pathErr.Err = os.ErrExist
		return pathErr
	}

	child = newInode(os.ModeDir | (perm &^ os.ModeType &^ fs.Umask()))
	err := fs.saveParentChild(parent, filename, child)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}
	return nil
}

// MkdirAll creates a directory named path, along with any necessary parents,
// and returns nil, or else returns an error. The permission bits perm (before
// umask) are used for all directories that MkdirAll creates. If path is already
// a directory, MkdirAll does nothing and returns nil.
func (fs *FS) MkdirAll(name string, perm os.FileMode) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.mkdirAll(name, perm)
}

func (fs *FS) mkdirAll(name string, perm os.FileMode) error {
	dir, filename := fs.cleanPath(name)
	name = strings.TrimLeft(path.Join(dir, filename), "/")

	// If name is empty (i.e., we're asked to create "/"), root already exists
	if name == "" {
		return nil
	}

	p := "/"
	for _, part := range strings.Split(name, "/") {
		// Skip empty parts (shouldn't happen after TrimLeft, but be safe)
		if part == "" {
			continue
		}
		p = path.Join(p, part)
		err := fs.mkdir(p, perm)
		if err != nil {
			patherr, ok := err.(*os.PathError)
			if !ok || patherr.Err != os.ErrExist {
				return err
			}
			// Mkdir reports ErrExist for any existing entry; only a directory
			// is an acceptable intermediate component.
			node, resolveErr := fs.resolve(p)
			if resolveErr != nil {
				return err
			}
			if !node.IsDir() {
				return &os.PathError{Op: "mkdir", Path: p, Err: syscall.ENOTDIR}
			}
		}
	}

	return nil
}

// Remove removes the named file or (empty) directory. If there is an error, it
// will be of type *PathError.
func (fs *FS) Remove(name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// The root cannot be removed.
	if name == "/" {
		return &os.PathError{Op: "remove", Path: name, Err: syscall.EBUSY}
	}

	pathErr := &os.PathError{Op: "remove", Path: name}
	dir, filename := fs.cleanPath(name)
	parent, child := fs.loadParentChild(dir, filename)
	if child == nil {
		pathErr.Err = os.ErrNotExist
		return pathErr
	}

	if child.IsDir() && len(child.Children) > 0 {
		pathErr.Err = syscall.ENOTEMPTY
		return pathErr
	}

	_, err := parent.Unlink(filename)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}

	_, err = fs.saveInode(parent)
	if err != nil {
		return err
	}

	if child.countDown() == 0 {
		err = fs.deleteInode(child.Ino)
		if err != nil {
			return err
		}
		return nil
	}
	_, err = fs.saveInode(child)
	return err
}

// RemoveAll removes path and any children it contains. It removes everything
// it can but returns the first error it encounters. If the path does not exist,
// RemoveAll returns nil (no error).
func (fs *FS) RemoveAll(name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	dir, filename := fs.cleanPath(name)
	parent, child := fs.loadParentChild(dir, filename)
	_, _ = parent, child
	var inos []uint64

	var rootid uint64
	if dir == "/" && parent != nil {
		rootid = parent.Ino
	}

	// Collect all inodes iteratively using ReadDir (avoids stack overflow for deep trees)
	// Use Lstat to not follow symlinks - when we hit a symlink, we just delete
	// the symlink itself, we don't traverse into directories it points to
	stack := []string{path.Join(dir, filename)}
	for len(stack) > 0 {
		// Pop from stack
		p := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		info, err := fs.lstat(p)
		if err != nil {
			if os.IsNotExist(err) {
				// A missing path (or a child that vanished concurrently) is
				// not an error for RemoveAll.
				return nil
			}
			return err
		}
		node, ok := info.Sys().(*iNode)
		if !ok {
			return errors.New("unable to cast os.FileInfo to *iNode")
		}
		inos = append(inos, node.Ino)

		// Only traverse into actual directories, not symlinks to directories
		if info.IsDir() && (info.Mode()&os.ModeSymlink == 0) {
			entries, err := fs.readDir(p)
			if err != nil {
				return err
			}
			for _, entry := range entries {
				childPath := path.Join(p, entry.Name())
				stack = append(stack, childPath)
			}
		}
	}

	if len(inos) == 0 {
		return nil
	}

	for i, j := 0, len(inos)-1; i < len(inos)/2; i, j = i+1, j-1 {
		inos[i], inos[j] = inos[j], inos[i]
	}

	// Delete content from external content filesystem first
	// We need to collect which inodes are regular files vs directories
	var fileInos []uint64
	var err error
	err = fs.db.View(func(tx *bolt.Tx) error {
		b, err := fs.openFsBucket(tx)
		if err != nil {
			return err
		}
		for _, ino := range inos {
			if rootid != 0 && ino == rootid {
				continue
			}
			node, err := b.GetInode(ino)
			if err != nil {
				continue // Node might already be deleted
			}
			// Only try to remove content for regular files, not directories
			if !node.IsDir() {
				fileInos = append(fileInos, ino)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	if fs.contentFS != nil {
		for _, ino := range fileInos {
			contentPath := inoToPath(ino)
			if err := fs.contentFS.Remove(contentPath); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}

	err = fs.db.Update(func(tx *bolt.Tx) error {
		b, err := fs.openFsBucket(tx)
		if err != nil {
			return err
		}

		for _, ino := range inos {
			if rootid != 0 && ino == rootid {
				continue
			}
			key := i2b(ino)
			err := b.inodes.Delete(key)
			if err != nil {
				return err
			}
			// Only delete from data bucket if no external content filesystem is configured
			if fs.contentFS == nil {
				err = b.data.Delete(key)
				if err != nil {
					return err
				}
			}
			// Remove chunked content and symlink targets to avoid leaks.
			b.symlinks.Delete(key)
			if err := b.deleteAllChunks(ino); err != nil {
				return err
			}
		}

		// Update the parent in the same transaction, so a failure cannot leave
		// the parent listing deleted inodes.
		if parent != nil {
			p, err := b.GetInode(parent.Ino)
			if err != nil {
				return err
			}
			if child != nil {
				for i, entry := range p.Children {
					if entry.Ino == child.Ino {
						p.Children = append(p.Children[:i], p.Children[i+1:]...)
						break
					}
				}
			} else {
				p.Children = p.Children[:0]
			}
			if err := b.PutInode(p.Ino, p); err != nil {
				return err
			}
			if fs.cache != nil {
				fs.cache.Invalidate(p.Ino)
				if child != nil {
					fs.cache.Invalidate(child.Ino)
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

// Chtimes changes the access and modification times of the named file
func (fs *FS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	dir, filename := fs.cleanPath(name)
	node, err := fs.resolveFollow(path.Join(dir, filename), 0)
	if err != nil {
		return err
	}

	node.SetAtime(atime)
	node.SetMtime(mtime)

	_, err = fs.saveInode(node)
	return err
}

// Chown changes the owner and group ids of the named file
func (fs *FS) Chown(name string, uid, gid int) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.chown(name, uid, gid, 0)
}

func (fs *FS) chown(name string, uid, gid int, depth int) error {
	pathErr := &os.PathError{Op: "chown", Path: name}
	if depth > maxSymlinkDepth {
		pathErr.Err = syscall.ELOOP
		return pathErr
	}

	dir, filename := fs.cleanPath(name)
	_, node := fs.loadParentChild(dir, filename)
	if node == nil {
		pathErr.Err = os.ErrNotExist
		return pathErr
	}

	if node.Mode&os.ModeSymlink == 0 {
		node.Uid = uint32(uid)
		node.Gid = uint32(gid)

		_, err := fs.saveInode(node)
		if err != nil {
			pathErr.Err = err
			return pathErr
		}
		return nil
	}
	link, err := fs.readLink(node.Ino)
	if err != nil {
		return err
	}
	if link == "" {
		pathErr.Err = os.ErrNotExist
		return pathErr
	}
	if !path.IsAbs(link) {
		// Relative symlinks are relative to the directory containing the symlink
		link = path.Join(path.Dir(name), link)
	}

	return fs.chown(link, uid, gid, depth+1)
}

// Chmod changes the mode of the named file to mode.
func (fs *FS) Chmod(name string, mode os.FileMode) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	dir, filename := fs.cleanPath(name)
	node, err := fs.resolveFollow(path.Join(dir, filename), 0)
	if err != nil {
		return err
	}

	// Preserve type bits (e.g. os.ModeDir) and only change permission bits.
	node.Mode = (node.Mode & os.ModeType) | (mode &^ os.ModeType)

	_, err = fs.saveInode(node)
	return err
}

// Lstat returns a FileInfo describing the named file. If the file is a symbolic
// link, the returned FileInfo describes the symbolic link. Lstat makes no
// attempt to follow the link. If there is an error, it will be of type
// *PathError.
func (fs *FS) Lstat(name string) (os.FileInfo, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.lstat(name)
}

// lstat is Lstat without the lock; callers must hold fs.mu.
func (fs *FS) lstat(name string) (os.FileInfo, error) {
	pathErr := &os.PathError{Op: "lstat", Path: name}
	dir, filename := fs.cleanPath(name)
	node, err := fs.resolve(path.Join(dir, filename))
	if err != nil {
		pathErr.Err = err
		return nil, pathErr
	}
	if filename == "" {
		filename = dir
	}
	return inodeinfo{filename, node}, nil
}

// Lchown changes the numeric uid and gid of the named file. If the file is a
// symbolic link, it changes the uid and gid of the link itself. If there is an
// error, it will be of type *PathError.
//
// On Windows, it always returns the syscall.EWINDOWS error, wrapped in *PathError.
func (fs *FS) Lchown(name string, uid, gid int) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	pathErr := &os.PathError{Op: "lchown", Path: name}
	dir, filename := fs.cleanPath(name)
	_, node := fs.loadParentChild(dir, filename)
	if node == nil {
		pathErr.Err = os.ErrNotExist
		return pathErr
	}
	node.Uid = uint32(uid)
	node.Gid = uint32(gid)

	_, err := fs.saveInode(node)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}
	return nil
}

// Readlink returns the destination of the named symbolic link. If there is an
// error, it will be of type *PathError.
func (fs *FS) Readlink(name string) (string, error) {
	node, err := fs.resolve(name)
	if err != nil {
		return "", err
	}

	link, err := fs.readLink(node.Ino)
	if err != nil {
		return "", err
	}
	if link == "" {
		return "", os.ErrNotExist
	}
	return link, nil
}

// Symlink creates newname as a symbolic link to oldname. If there is an error,
// it will be of type *LinkError.
func (fs *FS) Symlink(source, destination string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	pathErr := &os.PathError{Op: "symlink", Path: destination}

	dstDir, dstFilename := fs.cleanPath(destination)
	dstParent, dstChild := fs.loadParentChild(dstDir, dstFilename)

	if dstParent == nil {
		pathErr.Err = os.ErrNotExist
		pathErr.Path = dstDir
		return pathErr
	}

	if dstChild != nil {
		pathErr.Err = os.ErrExist
		pathErr.Path = destination
		return pathErr
	}

	node := newInode(os.ModeSymlink | (fs.Umask() &^ os.ModeType))
	node.countUp() // Increment link count since we're adding to parent directory

	ino, err := fs.saveInode(node)
	if err != nil {
		pathErr.Err = err
		pathErr.Path = destination
		return pathErr
	}

	err = fs.db.Update(func(tx *bolt.Tx) error {
		b, err := fs.openFsBucket(tx)
		if err != nil {
			return err
		}
		return b.Symlink(ino, source)
	})
	if err != nil {
		pathErr.Err = err
		return pathErr
	}

	_, err = dstParent.Link(dstFilename, ino)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}
	ino, err = fs.saveInode(dstParent)
	if err != nil {
		pathErr.Err = err
		pathErr.Path = destination
		return pathErr
	}

	return nil
}
