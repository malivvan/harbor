package fstree

import (
	"os"
	"path"
)

// ContentFS stores the file content of an FS outside its bbolt database, for
// example in an object store or on local disk.
//
// The FS reads and writes whole content objects: it never opens one for
// seeking, appending or partial updates. Paths are slash-separated virtual
// paths such as "/inodes/12/34"; implementations may map them onto keys,
// object names or directory trees.
//
// Content is sealed before it reaches the backend when content encryption is
// enabled, so a backend never receives plaintext in that mode.
type ContentFS interface {
	// ReadFile returns the whole content stored at name.
	//
	// A missing object must return an error satisfying os.IsNotExist, so the
	// FS can treat the file as empty. Any other error aborts the read.
	ReadFile(name string) ([]byte, error)

	// WriteFile replaces the whole content at name, creating any missing
	// parent directories or key prefixes. perm is applied when the backend
	// supports permission bits.
	//
	// The FS never calls WriteFile concurrently with another operation on the
	// same path.
	WriteFile(name string, data []byte, perm os.FileMode) error

	// Remove deletes the content at name. A missing object must return an
	// error satisfying os.IsNotExist.
	Remove(name string) error
}

// Compile-time assertion for the *FS adapter.
var _ ContentFS = fsContentFS{}

// fsContentFS adapts an *FS to ContentFS. The FS interface operates on whole
// objects too, so the adapter only has to turn OpenFile/Write/Close into
// WriteFile.
type fsContentFS struct {
	fs *FS
}

// AsContentFS returns an adapter that lets this file system store the content
// of another FS. It is the bridge for using an in-database or s3-backed FS as
// the external content backend of another file system:
//
//	content, _ := store.Open("content")
//	documents, _ := store.OpenWithContentFS("documents", content.AsContentFS())
func (fs *FS) AsContentFS() ContentFS {
	return fsContentFS{fs: fs}
}

func (c fsContentFS) ReadFile(name string) ([]byte, error) {
	return c.fs.ReadFile(name)
}

func (c fsContentFS) WriteFile(name string, data []byte, perm os.FileMode) error {
	if dir := path.Dir(name); dir != "." && dir != "/" {
		if err := c.fs.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	f, err := c.fs.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (c fsContentFS) Remove(name string) error {
	return c.fs.Remove(name)
}
