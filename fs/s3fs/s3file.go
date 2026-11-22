package s3fs

import (
	"bytes"
	"io"
	iosfs "io/fs"
	"os"
	"strings"
	"sync"
)

// File represents a file in S3.
// File instances are safe for concurrent use; all mutable state is
// protected by an internal mutex.
type File struct {
	mu      sync.Mutex
	fs      *FileSystem
	name    string
	key     string
	writing bool
	closed  bool
	append  bool
	buffer  []byte
	offset  int64
	flushed int64 // number of leading bytes already uploaded via multipart
	body    io.ReadCloser

	// Multipart upload state (issue #15)
	multipartID string
	parts       []CompletedPart
	partNum     int32

	// ReadAt cache (issue #17) - full object cached on first ReadAt
	readCache []byte
}

// Name returns the name of the file.
func (f *File) Name() string {
	return f.name
}

// Read reads from the S3 object at the current offset.
// On first call, the object is fetched from S3 and cached so that subsequent
// reads and seeks honour the file offset.
func (f *File) Read(b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.writing {
		return 0, os.ErrInvalid
	}

	if err := f.loadReadCacheLocked(); err != nil {
		return 0, err
	}

	if f.offset < 0 {
		f.offset = 0
	}
	if f.offset >= int64(len(f.readCache)) {
		return 0, io.EOF
	}

	n := copy(b, f.readCache[f.offset:])
	f.offset += int64(n)
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}

// loadReadCacheLocked fetches the full object into readCache if not already
// loaded. Must be called with f.mu held.
func (f *File) loadReadCacheLocked() error {
	if f.readCache != nil {
		return nil
	}
	output, err := f.fs.client.GetObject(f.fs.ctx, &GetObjectInput{
		Bucket: f.fs.bucket,
		Key:    f.key,
	})
	if err != nil {
		return wrapError("read", f.name, err)
	}
	defer output.Body.Close()

	data, err := io.ReadAll(output.Body)
	if err != nil {
		return wrapError("read", f.name, err)
	}
	f.readCache = data
	return nil
}

// ReadAt reads from the S3 object at a specific offset.
// On first call, the full object is fetched and cached in memory for
// efficient subsequent random access. For streaming reads of large files,
// use [File.Read] instead.
func (f *File) ReadAt(b []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.writing {
		return 0, os.ErrInvalid
	}
	if off < 0 {
		return 0, os.ErrInvalid
	}

	if err := f.loadReadCacheLocked(); err != nil {
		return 0, err
	}

	if off >= int64(len(f.readCache)) {
		return 0, io.EOF
	}

	n := copy(b, f.readCache[off:])
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}

// Write writes to the file buffer. The buffer is uploaded to S3 on Close.
// For large writes, multipart upload is used automatically when the buffer
// exceeds the configured part size (default 5MB).
func (f *File) Write(b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.writing {
		return 0, os.ErrInvalid
	}
	if f.offset < 0 {
		return 0, os.ErrInvalid
	}

	// Map the logical offset into the current (not-yet-flushed) buffer. Bytes
	// below flushed have already been uploaded and can no longer be rewritten.
	rel := f.offset - f.flushed
	if rel < 0 || rel > maxObjectSize-int64(len(b)) {
		return 0, os.ErrInvalid
	}
	end := int(rel) + len(b)
	if end > len(f.buffer) {
		grown := make([]byte, end)
		copy(grown, f.buffer)
		f.buffer = grown
	}

	n := copy(f.buffer[rel:], b)
	f.offset += int64(n)

	// Flush parts if buffer exceeds part size
	if err := f.flushPartsLocked(); err != nil {
		return 0, wrapError("write", f.name, err)
	}

	return n, nil
}

// flushPartsLocked flushes complete parts from the buffer via multipart upload.
// Must be called with f.mu held.
func (f *File) flushPartsLocked() error {
	partSize := f.fs.effectivePartSize()
	if int64(len(f.buffer)) < partSize {
		return nil
	}

	// Start multipart upload if not already started
	if f.multipartID == "" {
		output, err := f.fs.client.CreateMultipartUpload(f.fs.ctx, &CreateMultipartUploadInput{
			Bucket: f.fs.bucket,
			Key:    f.key,
		})
		if err != nil {
			return err
		}
		f.multipartID = output.UploadID
	}

	// Upload complete parts
	for int64(len(f.buffer)) >= partSize {
		f.partNum++
		part := f.buffer[:partSize]
		f.buffer = f.buffer[partSize:]
		f.flushed += partSize

		output, err := f.fs.client.UploadPart(f.fs.ctx, &UploadPartInput{
			Bucket:     f.fs.bucket,
			Key:        f.key,
			UploadID:   f.multipartID,
			PartNumber: f.partNum,
			Body:       bytes.NewReader(part),
		})
		if err != nil {
			// Abort on failure
			f.abortMultipartLocked()
			return err
		}

		f.parts = append(f.parts, CompletedPart{
			ETag:       output.ETag,
			PartNumber: f.partNum,
		})
	}

	return nil
}

// abortMultipartLocked aborts an active multipart upload.
// Must be called with f.mu held.
func (f *File) abortMultipartLocked() {
	if f.multipartID == "" {
		return
	}
	_, _ = f.fs.client.AbortMultipartUpload(f.fs.ctx, &AbortMultipartUploadInput{
		Bucket:   f.fs.bucket,
		Key:      f.key,
		UploadID: f.multipartID,
	})
	f.multipartID = ""
	f.parts = nil
	f.partNum = 0
}

// WriteAt writes to the buffer at a specific offset.
func (f *File) WriteAt(b []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.writing {
		return 0, os.ErrInvalid
	}
	if off < 0 {
		return 0, os.ErrInvalid
	}

	rel := off - f.flushed
	if rel < 0 || rel > maxObjectSize-int64(len(b)) {
		return 0, os.ErrInvalid
	}

	// Extend buffer if necessary
	if int(rel)+len(b) > len(f.buffer) {
		newBuf := make([]byte, int(rel)+len(b))
		copy(newBuf, f.buffer)
		f.buffer = newBuf
	}

	copy(f.buffer[rel:], b)
	return len(b), nil
}

// WriteString writes a string to the file.
func (f *File) WriteString(s string) (int, error) {
	// Write handles its own locking
	return f.Write([]byte(s))
}

// Close closes the file. For files opened for writing, the buffered content
// is uploaded to S3. If a multipart upload is in progress, the remaining
// buffer is uploaded as the final part and the upload is completed.
func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil
	}

	if f.body != nil {
		f.body.Close()
	}

	if !f.writing {
		f.closed = true
		return nil
	}

	// Multipart path: upload final part and complete
	if f.multipartID != "" {
		if len(f.buffer) > 0 || f.partNum == 0 {
			f.partNum++
			output, err := f.fs.client.UploadPart(f.fs.ctx, &UploadPartInput{
				Bucket:     f.fs.bucket,
				Key:        f.key,
				UploadID:   f.multipartID,
				PartNumber: f.partNum,
				Body:       bytes.NewReader(f.buffer),
			})
			if err != nil {
				f.abortMultipartLocked()
				return wrapError("close", f.name, err)
			}
			f.parts = append(f.parts, CompletedPart{
				ETag:       output.ETag,
				PartNumber: f.partNum,
			})
		}

		_, err := f.fs.client.CompleteMultipartUpload(f.fs.ctx, &CompleteMultipartUploadInput{
			Bucket:   f.fs.bucket,
			Key:      f.key,
			UploadID: f.multipartID,
			MultipartUpload: &CompletedMultipartUpload{
				Parts: f.parts,
			},
		})
		if err != nil {
			f.abortMultipartLocked()
			return wrapError("close", f.name, err)
		}
		f.closed = true
		return nil
	}

	// Simple path: single PutObject
	_, err := f.fs.client.PutObject(f.fs.ctx, &PutObjectInput{
		Bucket: f.fs.bucket,
		Key:    f.key,
		Body:   bytes.NewReader(f.buffer),
	})
	return wrapError("close", f.name, err)
}

// Seek sets the offset for the next read or write.
// SeekEnd is not supported for S3 files.
func (f *File) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch whence {
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	case io.SeekEnd:
		return 0, os.ErrInvalid
	}
	return f.offset, nil
}

// Stat returns file info.
func (f *File) Stat() (os.FileInfo, error) {
	return f.fs.Stat(f.name)
}

// Sync is a no-op for S3 (writes are synchronous).
func (f *File) Sync() error {
	return nil
}

// Truncate changes the size of the file buffer. It does not change the I/O offset.
// If a multipart upload is in progress, it is aborted since already-uploaded
// parts cannot be truncated.
func (f *File) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.writing {
		return os.ErrInvalid
	}

	if size < 0 || size > maxObjectSize {
		return os.ErrInvalid
	}

	// Already-flushed parts cannot be truncated, and aborting the upload would
	// discard the file's prefix. Refuse instead of silently losing data.
	if f.multipartID != "" || f.flushed > 0 {
		return &os.PathError{Op: "truncate", Path: f.name, Err: os.ErrInvalid}
	}

	if size < int64(len(f.buffer)) {
		f.buffer = f.buffer[:size]
	} else {
		newBuf := make([]byte, size)
		copy(newBuf, f.buffer)
		f.buffer = newBuf
	}
	return nil
}

// Readdir reads directory entries (lists objects with the file's key as prefix).
// If n > 0, Readdir returns at most n entries. If n <= 0, it returns all entries.
func (f *File) Readdir(n int) ([]os.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.writing && f.body == nil {
		output, err := f.fs.client.HeadObject(f.fs.ctx, &HeadObjectInput{
			Bucket: f.fs.bucket,
			Key:    f.key,
		})
		if err == nil && !strings.HasSuffix(f.key, "/") {
			return nil, &os.PathError{Op: "readdir", Path: f.name, Err: os.ErrInvalid}
		}
		_ = output
	}

	prefix := f.key
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	var infos []os.FileInfo
	seen := make(map[string]bool)

	var continuationToken string
	for {
		output, err := f.fs.client.ListObjectsV2(f.fs.ctx, &ListObjectsV2Input{
			Bucket:            f.fs.bucket,
			Prefix:            prefix,
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, wrapError("readdir", f.name, err)
		}

		for _, obj := range output.Contents {
			key := obj.Key
			if key == prefix {
				continue
			}

			name := strings.TrimPrefix(key, prefix)
			slashIdx := strings.Index(name, "/")

			var displayName string
			var isDir bool

			if slashIdx == -1 {
				displayName = name
				isDir = false
			} else if slashIdx == len(name)-1 {
				displayName = name[:slashIdx]
				isDir = true
			} else {
				displayName = name[:slashIdx]
				isDir = true
			}

			if seen[displayName] {
				continue
			}
			seen[displayName] = true

			infos = append(infos, &fileInfo{
				name:    displayName,
				size:    obj.Size,
				modTime: obj.LastModified,
				isDir:   isDir,
			})

			if n > 0 && len(infos) >= n {
				if len(infos) == 0 && n > 0 {
					return nil, io.EOF
				}
				return infos, nil
			}
		}

		if !output.IsTruncated {
			break
		}
		continuationToken = output.NextContinuationToken
	}

	if len(infos) == 0 && n > 0 {
		return nil, io.EOF
	}
	return infos, nil
}

// Readdirnames reads directory entry names.
func (f *File) Readdirnames(n int) ([]string, error) {
	infos, err := f.Readdir(n)
	if err != nil {
		return nil, err
	}

	names := make([]string, len(infos))
	for i, info := range infos {
		names[i] = info.Name()
	}
	return names, nil
}

// ReadDir reads the contents of the directory and returns a slice of up to n DirEntry values.
// If n > 0, ReadDir returns at most n entries. If n <= 0, it returns all entries.
func (f *File) ReadDir(n int) ([]iosfs.DirEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.writing && f.body == nil {
		output, err := f.fs.client.HeadObject(f.fs.ctx, &HeadObjectInput{
			Bucket: f.fs.bucket,
			Key:    f.key,
		})
		if err == nil && !strings.HasSuffix(f.key, "/") {
			return nil, &os.PathError{Op: "readdir", Path: f.name, Err: os.ErrInvalid}
		}
		_ = output
	}

	prefix := f.key
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	var entries []iosfs.DirEntry
	seen := make(map[string]bool)

	var continuationToken string
	for {
		output, err := f.fs.client.ListObjectsV2(f.fs.ctx, &ListObjectsV2Input{
			Bucket:            f.fs.bucket,
			Prefix:            prefix,
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, wrapError("readdir", f.name, err)
		}

		for _, obj := range output.Contents {
			key := obj.Key
			if key == prefix {
				continue
			}

			name := strings.TrimPrefix(key, prefix)
			slashIdx := strings.Index(name, "/")

			var displayName string
			var isDir bool

			if slashIdx == -1 {
				displayName = name
				isDir = false
			} else if slashIdx == len(name)-1 {
				displayName = name[:slashIdx]
				isDir = true
			} else {
				displayName = name[:slashIdx]
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

			if n > 0 && len(entries) >= n {
				if len(entries) == 0 && n > 0 {
					return nil, io.EOF
				}
				return entries, nil
			}
		}

		if !output.IsTruncated {
			break
		}
		continuationToken = output.NextContinuationToken
	}

	if len(entries) == 0 && n > 0 {
		return nil, io.EOF
	}
	return entries, nil
}
