package file

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/malivvan/harbor/internal/absfs"
)

// dirWriteFlags represents the flag combination (07200 octal) for a directory
// opened with write permissions, which should return EOF when read.
const dirWriteFlags = 3712

// maxFileSize bounds a single file so a huge offset/size cannot overflow the
// int arithmetic or trigger an enormous allocation.
const maxFileSize int64 = 1 << 40

// File implements the absfs.File interface, providing a file interface for boltdb
type File struct {
	fs *FS
	// db *bolt.DB

	name  string
	flags int
	node  *iNode

	offset    int64
	diroffset int
	closed    bool

	// data caches the decrypted whole-file content for non-chunked storage so
	// repeated writes do not reload and decrypt the file on every call. It is
	// invalidated by Truncate and by chunked I/O.
	data       []byte
	dataLoaded bool
}

// Name returns the name of the file as presented to Open.
func (f *File) Name() string {
	return f.name
}

// Read reads up to len(b) bytes from the File. It returns the number of bytes
// read and any error encountered. At end of file, Read returns 0, io.EOF.
func (f *File) Read(p []byte) (int, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	if f.flags == dirWriteFlags {
		return 0, io.EOF
	}
	if f.flags&absfs.O_ACCESS == os.O_WRONLY {
		return 0, &os.PathError{Op: "read", Path: f.name, Err: syscall.EBADF} //os.ErrPermission
	}
	if f.node.IsDir() && f.node.Size == 0 {
		return 0, &os.PathError{Op: "read", Path: f.name, Err: syscall.EISDIR} //os.ErrPermission
	}
	if f.offset < 0 {
		return 0, &os.PathError{Op: "read", Path: f.name, Err: os.ErrInvalid}
	}
	if f.fs.chunked() && f.fs.contentFS == nil {
		return f.readChunked(p)
	}
	offset := int(f.offset)

	data, err := f.loadCachedData()
	if err != nil {
		return 0, err
	}

	// Validate and sync node size with actual data size. For external content
	// a mismatch means the content and the metadata disagree (a failed write or
	// a deleted object); shrinking the logical file here would silently adopt
	// the shorter content as the truth.
	if f.node.Size != int64(len(data)) {
		if f.fs.contentFS != nil {
			return 0, &os.PathError{Op: "read", Path: f.name,
				Err: fmt.Errorf("inode size %d does not match external content size %d", f.node.Size, len(data))}
		}
		f.node.Size = int64(len(data))
		_, err := f.fs.saveInode(f.node)
		if err != nil {
			return 0, err
		}
	}

	if offset >= len(data) {
		return 0, io.EOF
	}

	n := copy(p, data[f.offset:])
	f.offset += int64(n)

	if err != nil {
		return n, err
	}

	return n, nil
}

// loadCachedData returns the decrypted content of a non-chunked file,
// loading it once per open handle.
func (f *File) loadCachedData() ([]byte, error) {
	if f.dataLoaded {
		return f.data, nil
	}
	data, err := f.fs.loadData(f.node.Ino)
	if err != nil {
		return nil, err
	}
	f.data = data
	f.dataLoaded = true
	return data, nil
}

// invalidateData drops the cached whole-file content.
func (f *File) invalidateData() {
	f.data = nil
	f.dataLoaded = false
}

// readChunked reads from encrypted chunks without loading the whole file.
func (f *File) readChunked(p []byte) (int, error) {
	if f.offset >= f.node.Size {
		return 0, io.EOF
	}

	want := int64(len(p))
	if f.offset+want > f.node.Size {
		want = f.node.Size - f.offset
	}
	data, err := f.fs.readRange(f.node.Ino, f.offset, want)
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

// ReadAt reads len(b) bytes from the File starting at byte offset off. It
// returns the number of bytes read and the error, if any. ReadAt always
// returns a non-nil error when n < len(b). At end of file, that error is
// io.EOF.
func (f *File) ReadAt(b []byte, off int64) (n int, err error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	if f.flags&absfs.O_ACCESS == os.O_WRONLY {
		return 0, os.ErrPermission
	}
	if off < 0 {
		return 0, &os.PathError{Op: "readat", Path: f.name, Err: os.ErrInvalid}
	}
	saved := f.offset
	f.offset = off
	n, err = f.Read(b)
	f.offset = saved
	return n, err
}

// Write writes len(p) bytes from p to the file at the current offset,
// growing the file as needed. It returns the number of bytes written and
// any error encountered. Write always writes the whole slice or returns an
// error.
func (f *File) Write(p []byte) (int, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	if f.flags&absfs.O_ACCESS == os.O_RDONLY {
		return 0, &os.PathError{Op: "write", Path: f.name, Err: syscall.EBADF}
	}
	if f.flags&os.O_APPEND != 0 {
		node, err := f.fs.loadInode(f.node.Ino)
		if err != nil {
			return 0, err
		}
		f.offset = node.Size
		f.node.Size = node.Size
	}
	if f.offset < 0 {
		return 0, &os.PathError{Op: "write", Path: f.name, Err: os.ErrInvalid}
	}
	if f.offset > maxFileSize-int64(len(p)) {
		return 0, &os.PathError{Op: "write", Path: f.name, Err: syscall.EFBIG}
	}

	if f.fs.chunked() && f.fs.contentFS == nil {
		// Commit the handle state only after the transaction succeeds. The
		// inode passed to the transaction must already carry the new size, so
		// restore it when the write fails and leave the offset untouched.
		start := f.offset
		end := start + int64(len(p))
		oldSize := f.node.Size
		if end > f.node.Size {
			f.node.Size = end
		}
		if err := f.fs.writeRangeAndInode(f.node.Ino, start, p, f.node); err != nil {
			f.node.Size = oldSize
			return 0, err
		}
		f.offset = end
		if err := f.fs.auditEntry("write", f.name, digestOf(p)); err != nil {
			return len(p), err
		}
		return len(p), nil
	}

	size := len(p) + int(f.offset)
	data, err := f.loadCachedData()
	if err != nil {
		return 0, err
	}

	if size > len(data) {
		d := make([]byte, size)
		copy(d, data)
		data = d
	}

	n := copy(data[int(f.offset):], p)
	f.data = data
	f.offset += int64(n)
	f.node.Size = int64(len(data))

	if f.fs.contentFS != nil {
		// Commit the content before the size-bearing inode. If the content
		// write fails, metadata still describes the previous, complete file;
		// the opposite order advertises bytes that never reached the store.
		if err := f.fs.saveData(f.node.Ino, data); err != nil {
			return 0, err
		}
		if _, err := f.fs.saveInode(f.node); err != nil {
			return 0, err
		}
		if err := f.fs.auditEntry("write", f.name, digestOf(p)); err != nil {
			return n, err
		}
		return n, nil
	}

	if _, err := f.fs.saveInodeAndData(f.node, data); err != nil {
		return n, err
	}
	if err := f.fs.auditEntry("write", f.name, digestOf(p)); err != nil {
		return n, err
	}

	return n, nil
}

// WriteAt writes len(b) bytes to the File starting at byte offset off. It
// returns the number of bytes written and an error, if any. WriteAt returns
// a non-nil error when n != len(b).
func (f *File) WriteAt(b []byte, off int64) (n int, err error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	if off < 0 || off > maxFileSize-int64(len(b)) {
		return 0, &os.PathError{Op: "writeat", Path: f.name, Err: os.ErrInvalid}
	}
	saved := f.offset
	f.offset = off
	n, err = f.Write(b)
	f.offset = saved
	return n, err
}

// Close closes the File, rendering it unusable for I/O. On files that support
// SetDeadline, any pending I/O operations will be canceled and return
// immediately with an error.
func (f *File) Close() error {
	if f.closed {
		return nil
	}
	if err := f.Sync(); err != nil {
		return err
	}
	f.closed = true
	f.node = nil
	return nil
}

// Seek sets the offset for the next Read or Write on file to offset,
// interpreted according to whence: 0 means relative to the origin of the file,
// 1 means relative to the current offset, and 2 means relative to the end. It
// returns the new offset and an error, if any. The behavior of Seek on a file
// opened with O_APPEND is not specified.
func (f *File) Seek(offset int64, whence int) (ret int64, err error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	switch whence {
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	case io.SeekEnd:
		f.offset = int64(f.node.Size) + offset
	default:
		return 0, &os.PathError{Op: "seek", Path: f.name, Err: syscall.EINVAL}
	}
	if f.offset < 0 {
		f.offset = 0
	}
	return f.offset, nil
}

// Stat returns the FileInfo structure describing file. If there is an error, it
// will be of type *PathError.
func (f *File) Stat() (os.FileInfo, error) {
	if f.closed {
		return nil, os.ErrClosed
	}
	return &fileinfo{filepath.Base(f.name), f.node}, nil
}

// Sync does nothing because Write always completes before returning
func (f *File) Sync() error {
	// if f.flags&absfs.O_ACCESS == os.O_RDONLY {
	// 	return nil
	// }
	// f.fs.data[int(f.node.Ino)] = f.data
	// f.node.Size = int64(len(f.data))
	return nil
}

// Readdir reads the contents of the directory associated with file and
// returns a slice of up to n FileInfo values, as would be returned by Lstat,
// in directory order. Subsequent calls on the same file will yield further
// FileInfos.
//
// If n > 0, Readdir returns at most n FileInfo structures. In this case,
// if Readdir returns an empty slice, it will return a non-nil error
// explaining why. At the end of a directory, the error is io.EOF.
//
// If n <= 0, Readdir returns all the FileInfo from the directory in a single
//
//	slice. In this case, if Readdir succeeds (reads all the way to the end of
//
// the directory), it returns the slice and a nil error. If it encounters an
// error before the end of the directory, Readdir returns the FileInfo read
// until that point and a non-nil error.
func (f *File) Readdir(n int) ([]os.FileInfo, error) {
	if f.closed {
		return nil, os.ErrClosed
	}
	if f.flags&absfs.O_ACCESS == os.O_WRONLY {
		return nil, os.ErrPermission
	}
	if !f.node.IsDir() {
		return nil, syscall.ENOTDIR
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

	endOffset := f.diroffset + n
	inos := make([]uint64, n)
	for i, entry := range children[f.diroffset:endOffset] {
		inos[i] = entry.Ino
	}
	nodes, err := f.fs.loadInodes(inos)
	if err != nil {
		return nil, err
	}

	infos := make([]os.FileInfo, n)
	for i, node := range nodes {
		infos[i] = &fileinfo{children[f.diroffset+i].Name, node}
	}
	f.diroffset = endOffset
	return infos, nil
}

// Readdirnames reads and returns a slice of names from the directory f.
//
// If n > 0, Readdirnames returns at most n names. In this case, if
// Readdirnames returns an empty slice, it will return a non-nil error
// explaining why. At the end of a directory, the error is io.EOF.
//
// If n <= 0, Readdirnames returns all the names from the directory in a single
// slice. In this case, if Readdirnames succeeds (reads all the way to the end
// of the directory), it returns the slice and a nil error. If it encounters an
// error before the end of the directory, Readdirnames returns the names read
// until that point and a non-nil error.
func (f *File) Readdirnames(n int) ([]string, error) {
	if f.closed {
		return nil, os.ErrClosed
	}
	if f.flags&absfs.O_ACCESS == os.O_WRONLY {
		return nil, os.ErrPermission
	}
	if !f.node.IsDir() {
		return nil, syscall.ENOTDIR
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

// Truncate changes the size of the named file. If the file is a symbolic link,
// it changes the size of the link's target. If there is an error, it will be of
// type *PathError.
func (f *File) Truncate(size int64) error {
	if f.closed {
		return os.ErrClosed
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	if f.flags&absfs.O_ACCESS == os.O_RDONLY {
		return os.ErrPermission
	}
	if size < 0 {
		return &os.PathError{Op: "truncate", Path: f.name, Err: syscall.EINVAL}
	}

	f.invalidateData()
	if err := f.fs.resizeContent(f.node, size); err != nil {
		return err
	}
	return f.fs.auditEntry("truncate", f.name, nil)
}

// WriteString is like Write, but writes the contents of string s rather than a
// slice of bytes.
func (f *File) WriteString(s string) (n int, err error) {
	return f.Write([]byte(s))
}

// ReadDir reads the contents of the directory and returns a slice of up to n
// DirEntry values in directory order. This is the modern Go 1.16+ equivalent
// of Readdir that returns lightweight DirEntry values instead of full FileInfo.
//
// If n > 0, ReadDir returns at most n entries. In this case, if ReadDir
// returns an empty slice, it will return a non-nil error explaining why.
// At the end of a directory, the error is io.EOF.
//
// If n <= 0, ReadDir returns all entries from the directory in a single slice.
// In this case, if ReadDir succeeds (reads all the way to the end of the
// directory), it returns the slice and a nil error.
func (f *File) ReadDir(n int) ([]fs.DirEntry, error) {
	if f.closed {
		return nil, os.ErrClosed
	}
	if f.flags&absfs.O_ACCESS == os.O_WRONLY {
		return nil, os.ErrPermission
	}
	if !f.node.IsDir() {
		return nil, syscall.ENOTDIR
	}
	children := f.node.Children
	remaining := len(children) - f.diroffset
	if n < 1 {
		if remaining <= 0 {
			return []fs.DirEntry{}, nil
		}
		n = remaining
	} else {
		if remaining <= 0 {
			return nil, io.EOF
		}
		if n > remaining {
			n = remaining
		}
	}

	endOffset := f.diroffset + n
	inos := make([]uint64, n)
	for i, entry := range children[f.diroffset:endOffset] {
		inos[i] = entry.Ino
	}
	nodes, err := f.fs.loadInodes(inos)
	if err != nil {
		return nil, err
	}

	entries := make([]fs.DirEntry, n)
	for i, node := range nodes {
		entries[i] = &dirEntry{
			name: children[f.diroffset+i].Name,
			node: node,
		}
	}
	f.diroffset = endOffset
	return entries, nil
}

type fileinfo struct {
	name string
	node *iNode
}

func (i *fileinfo) Name() string {
	return i.name
}

func (i *fileinfo) Size() int64 {
	return i.node.Size
}

func (i *fileinfo) ModTime() time.Time {
	return i.node.Mtime()
}

func (i *fileinfo) Mode() os.FileMode {
	return i.node.Mode
}

func (i *fileinfo) Sys() interface{} {
	return i.node
}

func (i *fileinfo) IsDir() bool {
	return i.node.IsDir()
}
