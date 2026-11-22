package absfs

import (
	"fmt"
	"io"
	"io/fs"
	"os"
)

// fileadapter - is an implementation of the `File` interface that wraps a
// `Seekable` type and uses the `Seekable` interface to implement the additional
// functions.
type fileadapter struct {
	sf Seekable
}

// Name - is a pass through function to the nested `Seekable` interface.
func (f *fileadapter) Name() string {
	return f.sf.Name()
}

// Read - is a pass through function to the nested `Seekable` interface.
func (f *fileadapter) Read(p []byte) (int, error) {
	return f.sf.Read(p)
}

// Readdir - is a pass through function to the nested `Seekable` interface.
func (f *fileadapter) Readdir(n int) ([]os.FileInfo, error) {
	return f.sf.Readdir(n)
}

// Sync - is a pass through function to the nested `Seekable` interface.
func (f *fileadapter) Sync() error {
	return f.sf.Sync()
}

// Stat - is a pass through function to the nested `Seekable` interface.
func (f *fileadapter) Stat() (os.FileInfo, error) {
	return f.sf.Stat()
}

// Seek - is a pass through function to the nested `Seekable` interface.
func (f *fileadapter) Seek(offset int64, whence int) (ret int64, err error) {
	return f.sf.Seek(offset, whence)
}

// ReadAt - first checks to see if the nested `Seekable` type provides it's
// own implementations of `ReadAt` and `WriteAt`. If not `ReadAt` is implemented
// with `Seek` and `Read`.
func (f *fileadapter) ReadAt(b []byte, off int64) (n int, err error) {
	if file, ok := f.sf.(ater); ok {
		return file.ReadAt(b, off)
	}
	cur, err := f.sf.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if _, err = f.sf.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}
	n, err = f.sf.Read(b)
	if _, seekErr := f.sf.Seek(cur, io.SeekStart); err == nil {
		err = seekErr
	}
	return n, err
}

// Stat - is a pass through function to the nested `Seekable` interface.
func (f *fileadapter) Write(p []byte) (int, error) {
	return f.sf.Write(p)
}

// WriteAt - first checks to see if the nested `Seekable` type provides it's
// own implementations of `ReadAt` and `WriteAt`. If not `WriteAt` is
// implemented with `Seek` and `Write`.
func (f *fileadapter) WriteAt(b []byte, off int64) (n int, err error) {
	if file, ok := f.sf.(ater); ok {
		return file.WriteAt(b, off)
	}
	cur, err := f.sf.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if _, err = f.sf.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}
	n, err = f.sf.Write(b)
	if _, seekErr := f.sf.Seek(cur, io.SeekStart); err == nil {
		err = seekErr
	}
	return n, err
}

// WriteString - is a convenience function that calls `Write` with `s` after
// casting it to []byte.
func (f *fileadapter) WriteString(s string) (n int, err error) {
	return f.sf.Write([]byte(s))
}

// Truncate - first checks to see of the nested `Seekable` type provides it's
// own implementation of `Truncate`.  If not `Truncate` first seeks to the
// beginning of the file, then writes `size` zero value bytes.
func (f *fileadapter) Truncate(size int64) error {
	if file, ok := f.sf.(filetruncater); ok {
		return file.Truncate(size)
	}

	// Without a native Truncate the file can only be grown. Shrinking would
	// leave the tail intact, so reject it instead of silently corrupting the
	// file.
	cur, err := f.sf.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if size < cur {
		return fmt.Errorf("absfs: cannot truncate %d to %d bytes: underlying file has no Truncate", cur, size)
	}
	if size == cur {
		return nil
	}

	// Grow by appending zeros; seeking to 0 first would overwrite existing
	// content.
	if _, err := f.sf.Seek(cur, io.SeekStart); err != nil {
		return err
	}
	remaining := size - cur
	buf := make([]byte, 4096)
	for remaining > 0 {
		n := int64(len(buf))
		if remaining < n {
			n = remaining
		}
		if _, err := f.sf.Write(buf[:n]); err != nil {
			return err
		}
		remaining -= n
	}
	return nil
}

// Readdirnames - first checks to see of the nested `Seekable` type provides
// it's own implementation of `Readdirnames`.  If not `Readdirnames` calls
// `Readdir` and returns the names as given by `Name()` for each
// `os.FileInfo` returned.
func (f *fileadapter) Readdirnames(n int) (names []string, err error) {
	if file, ok := f.sf.(dirnamer); ok {
		return file.Readdirnames(n)
	}

	var infos []os.FileInfo
	infos, err = f.sf.Readdir(n)
	if err != nil {
		return nil, err
	}

	names = make([]string, len(infos))
	for i, info := range infos {
		names[i] = info.Name()
	}

	return names, nil
}

// Close - is a pass through function to the nested `Seekable` interface.
func (f *fileadapter) Close() error {
	return f.sf.Close()
}

// ReadDir - first checks to see if the nested `Seekable` type provides its
// own implementation of `ReadDir`. If not, `ReadDir` calls `Readdir` and
// converts the []os.FileInfo to []fs.DirEntry.
func (f *fileadapter) ReadDir(n int) ([]fs.DirEntry, error) {
	if file, ok := f.sf.(dirEntryReader); ok {
		return file.ReadDir(n)
	}

	// Fallback: use Readdir and convert to DirEntry
	infos, err := f.sf.Readdir(n)
	if err != nil {
		return nil, err
	}

	entries := make([]fs.DirEntry, len(infos))
	for i, info := range infos {
		entries[i] = fs.FileInfoToDirEntry(info)
	}

	return entries, nil
}

// interfaces for easy interface typing

type ater interface {
	ReadAt(b []byte, off int64) (n int, err error)
	WriteAt(b []byte, off int64) (n int, err error)
}

type filetruncater interface {
	Truncate(size int64) error
}

type dirnamer interface {
	Readdirnames(n int) (names []string, err error)
}

type dirEntryReader interface {
	ReadDir(n int) ([]fs.DirEntry, error)
}
