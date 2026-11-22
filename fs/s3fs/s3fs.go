// Package s3fs implements an [absfs.Filer] backed by S3-compatible object storage.
//
// Directories are represented as zero-byte objects with a trailing slash.
// Write operations are buffered in memory and uploaded to S3 on [File.Close].
// The FileSystem is safe for concurrent use; individual [File] instances are
// also safe for concurrent use as of v1.1.
//
// S3 does not support POSIX permissions, ownership, or timestamps, so
// [FileSystem.Chmod], [FileSystem.Chtimes], and [FileSystem.Chown] return
// [absfs.ErrNotImplemented].
package s3fs

import (
	"bytes"
	"context"
	"errors"
	"io"
	iosfs "io/fs"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/malivvan/harbor/internal/absfs"
)

// DefaultPartSize is the buffer threshold for switching to multipart upload.
// S3 requires a minimum part size of 5MB for all parts except the last.
const DefaultPartSize = 5 * 1024 * 1024 // 5MB

// maxObjectSize bounds in-memory buffers and offsets for one object.
const maxObjectSize int64 = 1 << 40

// FileSystem implements absfs.Filer for S3 object storage.
type FileSystem struct {
	client   S3Client
	bucket   string
	ctx      context.Context
	partSize int64 // multipart part size; 0 means DefaultPartSize
}

// Config contains the configuration for connecting to S3.
type Config struct {
	Bucket       string // S3 bucket name
	Region       string // AWS region (defaults to AWS_REGION, then us-east-1)
	Endpoint     string // Optional custom endpoint, e.g. "http://localhost:9000"
	UsePathStyle bool   // Force path-style addressing for the default AWS endpoint
	AccessKey    string // Static credentials; fall back to the environment and ~/.aws/credentials
	SecretKey    string
	SessionToken string
	HTTPClient   *http.Client // Optional HTTP client (defaults to http.DefaultClient)
	PartSize     int64        // Multipart upload part size (0 means DefaultPartSize)
}

func (fs *FileSystem) effectivePartSize() int64 {
	if fs.partSize > 0 {
		return fs.partSize
	}
	return DefaultPartSize
}

// New creates a new S3 filesystem with the given configuration.
func New(cfg *Config) (*FileSystem, error) {
	if cfg == nil || cfg.Bucket == "" {
		return nil, errors.New("s3fs: bucket is required")
	}

	client, err := newHTTPClient(cfg)
	if err != nil {
		return nil, err
	}

	return &FileSystem{
		client:   client,
		bucket:   cfg.Bucket,
		ctx:      context.Background(),
		partSize: cfg.PartSize,
	}, nil
}

// NewWithClient creates a new S3 filesystem with a custom S3 client.
// This is primarily useful for testing with mock clients.
func NewWithClient(client S3Client, bucket string) *FileSystem {
	return &FileSystem{
		client: client,
		bucket: bucket,
		ctx:    context.Background(),
	}
}

// NewWithClientAndContext creates a new S3 filesystem with a custom S3 client and context.
func NewWithClientAndContext(ctx context.Context, client S3Client, bucket string) *FileSystem {
	return &FileSystem{
		client: client,
		bucket: bucket,
		ctx:    ctx,
	}
}

// sanitizePath cleans and validates a path for S3 operations.
// It normalizes "." and ".." components and rejects paths that would
// escape above the root (e.g., "../secret").
func sanitizePath(p string) (string, error) {
	// Detect traversal on the raw input before cleaning: path.Clean("/"+p)
	// always begins with "/", so any escaping ".." is silently collapsed and
	// can never be detected afterwards. Walk the components keeping a depth
	// counter and reject any ".." that would move above the root.
	depth := 0
	for _, part := range strings.Split(p, "/") {
		switch part {
		case "", ".":
			// no-op
		case "..":
			if depth == 0 {
				return "", &os.PathError{Op: "sanitize", Path: p, Err: os.ErrInvalid}
			}
			depth--
		default:
			depth++
		}
	}

	clean := path.Clean("/" + p)
	clean = strings.TrimPrefix(clean, "/")
	if clean == "." {
		clean = ""
	}
	return clean, nil
}

// WithContext returns a copy of the FileSystem that uses the given context
// for all S3 operations.
func (fs *FileSystem) WithContext(ctx context.Context) *FileSystem {
	return &FileSystem{
		client:   fs.client,
		bucket:   fs.bucket,
		ctx:      ctx,
		partSize: fs.partSize,
	}
}

// wrapError wraps S3 errors to be compatible with os.IsNotExist and os.IsExist.
func wrapError(op, path string, err error) error {
	if err == nil {
		return nil
	}

	// Map missing objects to os.ErrNotExist.
	if errors.Is(err, ErrNotFound) {
		return &os.PathError{Op: op, Path: path, Err: os.ErrNotExist}
	}

	// For other errors, wrap them in PathError
	return &os.PathError{Op: op, Path: path, Err: err}
}

// OpenFile opens the named file in S3.
//
// Supported flags: O_RDONLY, O_WRONLY, O_RDWR, O_CREATE, O_EXCL, O_TRUNC, O_APPEND.
// When O_APPEND is set, existing content is loaded and new writes append to it.
// When O_TRUNC is set (or O_CREATE for a new file), the file starts empty.
// Paths are sanitized to prevent traversal attacks.
func (fs *FileSystem) OpenFile(name string, flag int, perm os.FileMode) (absfs.File, error) {
	var err error
	name, err = sanitizePath(name)
	if err != nil {
		return nil, err
	}

	// Check if O_EXCL is set - file must not exist
	if flag&os.O_EXCL != 0 && flag&os.O_CREATE != 0 {
		_, err := fs.client.HeadObject(fs.ctx, &HeadObjectInput{
			Bucket: fs.bucket,
			Key:    name,
		})
		if err == nil {
			// File exists, return error
			return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrExist}
		}
		// File doesn't exist, which is what we want for O_EXCL
		if !errors.Is(err, ErrNotFound) {
			// Some other error occurred
			return nil, wrapError("open", name, err)
		}
	}

	// For write operations
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE) != 0 {
		// Check if trying to write to a directory
		dirKey := name
		if !strings.HasSuffix(dirKey, "/") {
			dirKey += "/"
		}

		output, err := fs.client.HeadObject(fs.ctx, &HeadObjectInput{
			Bucket: fs.bucket,
			Key:    dirKey,
		})
		if err == nil {
			_ = output
			return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrInvalid}
		}

		var buf []byte

		// Load existing content unless O_TRUNC is set. Only a genuinely
		// missing object starts empty; any other error must abort the open so
		// Close cannot overwrite the existing object with a truncated buffer.
		if flag&os.O_TRUNC == 0 {
			getOutput, getErr := fs.client.GetObject(fs.ctx, &GetObjectInput{
				Bucket: fs.bucket,
				Key:    name,
			})
			switch {
			case getErr == nil:
				buf, getErr = io.ReadAll(getOutput.Body)
				closeErr := getOutput.Body.Close()
				if getErr != nil {
					return nil, wrapError("open", name, getErr)
				}
				if closeErr != nil {
					return nil, wrapError("open", name, closeErr)
				}
			case errors.Is(getErr, ErrNotFound):
				// New object: start with an empty buffer.
			default:
				return nil, wrapError("open", name, getErr)
			}
		}

		if buf == nil {
			buf = []byte{}
		}

		f := &File{
			fs:      fs,
			name:    name,
			key:     name,
			writing: true,
			buffer:  buf,
		}

		if flag&os.O_APPEND != 0 {
			f.append = true
			f.offset = int64(len(buf))
		}

		return f, nil
	}

	// For read operations, verify the file or directory exists
	_, err = fs.client.HeadObject(fs.ctx, &HeadObjectInput{
		Bucket: fs.bucket,
		Key:    name,
	})
	if err != nil {
		// If not found, check if it's a directory (with trailing slash)
		if errors.Is(err, ErrNotFound) {
			dirKey := name
			if !strings.HasSuffix(dirKey, "/") {
				dirKey += "/"
			}
			_, dirErr := fs.client.HeadObject(fs.ctx, &HeadObjectInput{
				Bucket: fs.bucket,
				Key:    dirKey,
			})
			if dirErr != nil {
				// Neither file nor directory marker exists
				return nil, wrapError("open", name, err)
			}
			// Directory marker exists, open it for reading
			return &File{
				fs:      fs,
				name:    name,
				key:     name,
				writing: false,
			}, nil
		}
		return nil, wrapError("open", name, err)
	}

	return &File{
		fs:      fs,
		name:    name,
		key:     name,
		writing: false,
	}, nil
}

// Mkdir creates a directory in S3 by writing a zero-byte object with a trailing slash.
// Returns os.ErrExist if the directory already exists.
func (fs *FileSystem) Mkdir(name string, perm os.FileMode) error {
	var err error
	name, err = sanitizePath(name)
	if err != nil {
		return err
	}
	if !strings.HasSuffix(name, "/") {
		name += "/"
	}

	// An object at the same path is a file, not a directory.
	if _, fileErr := fs.client.HeadObject(fs.ctx, &HeadObjectInput{
		Bucket: fs.bucket,
		Key:    strings.TrimSuffix(name, "/"),
	}); fileErr == nil {
		return &os.PathError{Op: "mkdir", Path: name, Err: os.ErrExist}
	} else if !errors.Is(fileErr, ErrNotFound) {
		return wrapError("mkdir", name, fileErr)
	}

	// Check if directory already exists
	_, err = fs.client.HeadObject(fs.ctx, &HeadObjectInput{
		Bucket: fs.bucket,
		Key:    name,
	})
	if err == nil {
		// Directory already exists
		return &os.PathError{Op: "mkdir", Path: name, Err: os.ErrExist}
	}

	// Only proceed if the object is missing.
	if !errors.Is(err, ErrNotFound) {
		return wrapError("mkdir", name, err)
	}

	_, err = fs.client.PutObject(fs.ctx, &PutObjectInput{
		Bucket: fs.bucket,
		Key:    name,
		Body:   strings.NewReader(""),
	})
	return wrapError("mkdir", name, err)
}

// listObjects returns every object whose key starts with prefix, following
// pagination.
func (fs *FileSystem) listObjects(prefix string) ([]Object, error) {
	var objects []Object
	var token string
	for {
		output, err := fs.client.ListObjectsV2(fs.ctx, &ListObjectsV2Input{
			Bucket:            fs.bucket,
			Prefix:            prefix,
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}
		objects = append(objects, output.Contents...)
		if !output.IsTruncated {
			return objects, nil
		}
		token = output.NextContinuationToken
	}
}

// Remove removes a file or directory from S3.
// Returns os.ErrNotExist if neither the file nor a directory marker exists.
// For directories, it removes the directory marker (key with trailing slash).
func (fs *FileSystem) Remove(name string) error {
	var err error
	name, err = sanitizePath(name)
	if err != nil {
		return err
	}

	// Check if the object exists as a file
	_, headErr := fs.client.HeadObject(fs.ctx, &HeadObjectInput{
		Bucket: fs.bucket,
		Key:    name,
	})

	// If not a file, check as directory marker
	dirKey := name
	if !strings.HasSuffix(dirKey, "/") {
		dirKey += "/"
	}
	isDirMarker := false
	if headErr != nil {
		_, dirErr := fs.client.HeadObject(fs.ctx, &HeadObjectInput{
			Bucket: fs.bucket,
			Key:    dirKey,
		})
		if dirErr != nil {
			// Neither file nor directory exists
			return &os.PathError{Op: "remove", Path: name, Err: os.ErrNotExist}
		}
		isDirMarker = true
	}

	if isDirMarker {
		// Removing a directory only succeeds when nothing lives below its
		// prefix; otherwise the children would be orphaned while the call
		// reported success.
		objects, listErr := fs.listObjects(dirKey)
		if listErr != nil {
			return wrapError("remove", name, listErr)
		}
		for _, obj := range objects {
			if obj.Key == dirKey {
				continue
			}
			return &os.PathError{Op: "remove", Path: name, Err: syscall.ENOTEMPTY}
		}
		_, err = fs.client.DeleteObject(fs.ctx, &DeleteObjectInput{
			Bucket: fs.bucket,
			Key:    dirKey,
		})
		return wrapError("remove", name, err)
	}

	// Delete the file
	_, err = fs.client.DeleteObject(fs.ctx, &DeleteObjectInput{
		Bucket: fs.bucket,
		Key:    name,
	})
	if err != nil {
		return wrapError("remove", name, err)
	}

	// Also clean up directory marker if present
	if !strings.HasSuffix(name, "/") {
		_, _ = fs.client.DeleteObject(fs.ctx, &DeleteObjectInput{
			Bucket: fs.bucket,
			Key:    dirKey,
		})
	}

	return nil
}

// Rename moves a file in S3 by copying to the new key and deleting the old one.
// This is not atomic. If the delete fails after a successful copy, the copy is
// rolled back (best-effort).
func (fs *FileSystem) Rename(oldpath, newpath string) error {
	var err error
	oldpath, err = sanitizePath(oldpath)
	if err != nil {
		return err
	}
	newpath, err = sanitizePath(newpath)
	if err != nil {
		return err
	}

	if oldpath == newpath {
		return nil
	}

	// A directory is either a trailing-slash marker or has children below its
	// prefix. Move all of them, not just the marker.
	oldPrefix := oldpath + "/"
	objects, listErr := fs.listObjects(oldPrefix)
	if listErr != nil {
		return wrapError("rename", oldpath, listErr)
	}
	if len(objects) == 0 {
		_, headErr := fs.client.HeadObject(fs.ctx, &HeadObjectInput{
			Bucket: fs.bucket,
			Key:    oldPrefix,
		})
		switch {
		case headErr == nil:
			objects = []Object{{Key: oldPrefix}}
		case errors.Is(headErr, ErrNotFound):
			// Not a directory; fall through to the single-object path.
		default:
			return wrapError("rename", oldpath, headErr)
		}
	}
	if len(objects) > 0 {
		newPrefix := newpath + "/"
		// Copy first so a failure never leaves the tree half-deleted.
		for _, obj := range objects {
			target := newPrefix + strings.TrimPrefix(obj.Key, oldPrefix)
			_, err := fs.client.CopyObject(fs.ctx, &CopyObjectInput{
				Bucket:     fs.bucket,
				CopySource: "/" + fs.bucket + "/" + obj.Key,
				Key:        target,
			})
			if err != nil {
				return wrapError("rename", obj.Key, err)
			}
		}
		for _, obj := range objects {
			if _, err := fs.client.DeleteObject(fs.ctx, &DeleteObjectInput{
				Bucket: fs.bucket,
				Key:    obj.Key,
			}); err != nil {
				return wrapError("rename", obj.Key, err)
			}
		}
		return nil
	}

	// Copy object to new location.
	// CopySource must be in /bucket/key form (with leading slash).
	copySource := "/" + fs.bucket + "/" + oldpath
	_, err = fs.client.CopyObject(fs.ctx, &CopyObjectInput{
		Bucket:     fs.bucket,
		CopySource: copySource,
		Key:        newpath,
	})
	if err != nil {
		return wrapError("rename", oldpath, err)
	}

	// Delete old object
	_, err = fs.client.DeleteObject(fs.ctx, &DeleteObjectInput{
		Bucket: fs.bucket,
		Key:    oldpath,
	})
	if err != nil {
		// Rollback: delete the copy we just made
		_, _ = fs.client.DeleteObject(fs.ctx, &DeleteObjectInput{
			Bucket: fs.bucket,
			Key:    newpath,
		})
		return wrapError("rename", oldpath, err)
	}

	return nil
}

// Stat returns file info for an S3 object.
func (fs *FileSystem) Stat(name string) (os.FileInfo, error) {
	var err error
	name, err = sanitizePath(name)
	if err != nil {
		return nil, err
	}

	// Try the exact path first
	output, err := fs.client.HeadObject(fs.ctx, &HeadObjectInput{
		Bucket: fs.bucket,
		Key:    name,
	})
	if err == nil {
		// Issue #12 fix: use the decoded content length and modification time.
		return &fileInfo{
			name:    path.Base(name),
			size:    output.ContentLength,
			modTime: output.LastModified,
			isDir:   strings.HasSuffix(name, "/"),
		}, nil
	}

	// If not found and doesn't have trailing slash, try with trailing slash (directory)
	if errors.Is(err, ErrNotFound) && !strings.HasSuffix(name, "/") {
		dirKey := name + "/"
		output, dirErr := fs.client.HeadObject(fs.ctx, &HeadObjectInput{
			Bucket: fs.bucket,
			Key:    dirKey,
		})
		if dirErr == nil {
			return &fileInfo{
				name:    path.Base(name),
				size:    output.ContentLength,
				modTime: output.LastModified,
				isDir:   true,
			}, nil
		}
	}

	return nil, wrapError("stat", name, err)
}

// Chmod is not supported for S3.
func (fs *FileSystem) Chmod(name string, mode os.FileMode) error {
	return absfs.ErrNotImplemented
}

// Chtimes is not supported for S3.
func (fs *FileSystem) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return absfs.ErrNotImplemented
}

// Chown is not supported for S3.
func (fs *FileSystem) Chown(name string, uid, gid int) error {
	return absfs.ErrNotImplemented
}

// Truncate truncates a file to the specified size.
// For S3, this requires reading the file, truncating the data, and writing it back.
func (fs *FileSystem) Truncate(name string, size int64) error {
	var err error
	name, err = sanitizePath(name)
	if err != nil {
		return err
	}
	if size < 0 || size > maxObjectSize {
		return &os.PathError{Op: "truncate", Path: name, Err: os.ErrInvalid}
	}

	// Read the existing file content
	output, err := fs.client.GetObject(fs.ctx, &GetObjectInput{
		Bucket: fs.bucket,
		Key:    name,
	})
	if err != nil {
		return wrapError("truncate", name, err)
	}
	defer output.Body.Close()

	// Read the content
	content, err := io.ReadAll(output.Body)
	if err != nil {
		return wrapError("truncate", name, err)
	}

	// Truncate the content
	var truncated []byte
	if size < int64(len(content)) {
		truncated = content[:size]
	} else {
		// Extend with zeros if size is larger
		truncated = make([]byte, size)
		copy(truncated, content)
	}

	// Write back the truncated content
	_, err = fs.client.PutObject(fs.ctx, &PutObjectInput{
		Bucket: fs.bucket,
		Key:    name,
		Body:   bytes.NewReader(truncated),
	})
	return wrapError("truncate", name, err)
}

// fileInfo implements os.FileInfo for S3 objects.
type fileInfo struct {
	name    string
	size    int64
	modTime time.Time
	isDir   bool
}

func (fi *fileInfo) Name() string { return fi.name }
func (fi *fileInfo) Size() int64  { return fi.size }
func (fi *fileInfo) Mode() os.FileMode {
	if fi.isDir {
		return os.ModeDir | 0755
	}
	return 0644
}
func (fi *fileInfo) ModTime() time.Time { return fi.modTime }
func (fi *fileInfo) IsDir() bool        { return fi.isDir }
func (fi *fileInfo) Sys() interface{}   { return nil }

// Type implements iosfs.DirEntry.
func (fi *fileInfo) Type() iosfs.FileMode {
	if fi.isDir {
		return iosfs.ModeDir
	}
	return 0
}

// Info implements iosfs.DirEntry.
func (fi *fileInfo) Info() (iosfs.FileInfo, error) {
	return fi, nil
}

// ReadDir reads the named directory and returns a list of directory entries sorted by filename.
func (fs *FileSystem) ReadDir(name string) ([]iosfs.DirEntry, error) {
	var err error
	name, err = sanitizePath(name)
	if err != nil {
		return nil, err
	}

	// For root or empty, list with empty prefix
	prefix := name
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	var entries []iosfs.DirEntry
	seen := make(map[string]bool)

	var continuationToken string
	for {
		output, err := fs.client.ListObjectsV2(fs.ctx, &ListObjectsV2Input{
			Bucket:            fs.bucket,
			Prefix:            prefix,
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, wrapError("readdir", name, err)
		}

		for _, obj := range output.Contents {
			key := obj.Key
			if key == prefix {
				continue
			}

			relName := strings.TrimPrefix(key, prefix)
			slashIdx := strings.Index(relName, "/")

			var displayName string
			var isDir bool

			if slashIdx == -1 {
				displayName = relName
				isDir = false
			} else if slashIdx == len(relName)-1 {
				displayName = relName[:slashIdx]
				isDir = true
			} else {
				displayName = relName[:slashIdx]
				isDir = true
			}

			if seen[displayName] {
				continue
			}
			seen[displayName] = true

			entries = append(entries, &fileInfo{
				name:    displayName,
				size:    obj.Size,
				modTime: obj.LastModified,
				isDir:   isDir,
			})
		}

		if !output.IsTruncated {
			break
		}
		continuationToken = output.NextContinuationToken
	}

	// A missing path and a regular file must not look like an empty directory.
	// Checked after listing so implicit directories (objects below a prefix but
	// no marker) still work.
	if len(entries) == 0 && name != "" {
		if _, err := fs.client.HeadObject(fs.ctx, &HeadObjectInput{Bucket: fs.bucket, Key: name}); err == nil {
			return nil, &os.PathError{Op: "readdir", Path: name, Err: syscall.ENOTDIR}
		}
		if _, err := fs.client.HeadObject(fs.ctx, &HeadObjectInput{Bucket: fs.bucket, Key: prefix}); err != nil {
			return nil, &os.PathError{Op: "readdir", Path: name, Err: os.ErrNotExist}
		}
	}

	// Display names are derived by trimming at "/", so listing order is not
	// necessarily lexical (for example "a.txt" before "a"). Sort explicitly.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	return entries, nil
}

// ReadFile reads the named file and returns its contents.
func (fs *FileSystem) ReadFile(name string) ([]byte, error) {
	var err error
	name, err = sanitizePath(name)
	if err != nil {
		return nil, err
	}

	output, err := fs.client.GetObject(fs.ctx, &GetObjectInput{
		Bucket: fs.bucket,
		Key:    name,
	})
	if err != nil {
		return nil, wrapError("readfile", name, err)
	}
	defer output.Body.Close()

	return io.ReadAll(output.Body)
}

// Sub returns an fs.FS corresponding to the subtree rooted at dir.
func (fs *FileSystem) Sub(dir string) (iosfs.FS, error) {
	dir = strings.TrimPrefix(dir, "/")
	return absfs.FilerToFS(fs, dir)
}

// subFS wraps a FileSystem to operate within a subdirectory.
type subFS struct {
	parent *FileSystem
	prefix string
}

func (s *subFS) fullPath(name string) string {
	name = strings.TrimPrefix(name, "/")
	if s.prefix == "" {
		return name
	}
	if name == "" {
		return s.prefix
	}
	return path.Join(s.prefix, name)
}

func (s *subFS) OpenFile(name string, flag int, perm os.FileMode) (absfs.File, error) {
	return s.parent.OpenFile(s.fullPath(name), flag, perm)
}

func (s *subFS) Mkdir(name string, perm os.FileMode) error {
	return s.parent.Mkdir(s.fullPath(name), perm)
}

func (s *subFS) Remove(name string) error {
	return s.parent.Remove(s.fullPath(name))
}

func (s *subFS) Rename(oldpath, newpath string) error {
	return s.parent.Rename(s.fullPath(oldpath), s.fullPath(newpath))
}

func (s *subFS) Stat(name string) (os.FileInfo, error) {
	return s.parent.Stat(s.fullPath(name))
}

func (s *subFS) Chmod(name string, mode os.FileMode) error {
	return s.parent.Chmod(s.fullPath(name), mode)
}

func (s *subFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return s.parent.Chtimes(s.fullPath(name), atime, mtime)
}

func (s *subFS) Chown(name string, uid, gid int) error {
	return s.parent.Chown(s.fullPath(name), uid, gid)
}

func (s *subFS) ReadDir(name string) ([]iosfs.DirEntry, error) {
	return s.parent.ReadDir(s.fullPath(name))
}

func (s *subFS) ReadFile(name string) ([]byte, error) {
	return s.parent.ReadFile(s.fullPath(name))
}

func (s *subFS) Sub(dir string) (iosfs.FS, error) {
	fullDir := s.fullPath(dir)
	// The new wrapper already prefixes paths with fullDir; the adapter root is
	// relative to it.
	return absfs.FilerToFS(&subFS{parent: s.parent, prefix: fullDir}, "/")
}
