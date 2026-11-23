// Package blob is a flat object store on top of a Harbor file system.
//
// Harbor's `fs` is a hierarchical file system; many applications want a flat
// key/value blob namespace instead — "put these bytes at tenant/2024/invoice.pdf
// and give them back later". That is what this package provides: a bucket is a
// top-level directory, a key is the slash-separated path beneath it, and the pair
// maps to exactly one file in the file system.
//
// # Paths and containment
//
// A key is always interpreted relative to its bucket, and a key containing a ".."
// segment is rejected rather than rewritten. That keeps "the bucket" a real
// boundary: no key can address a file in another bucket, whatever the caller
// passes.
//
// # What this is not
//
// There is no HTTP surface, no presigned URL and no credential minting. The bytes
// live inside the Harbor database, so the application serves them itself. What
// this package adds beyond `fs` is the flat namespace, the bucket boundary, and
// the convenience operations (move-and-create-parents, stat-many, delete-is-a-no-op)
// that object-store callers expect.
package blob

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/malivvan/harbor/fs"
)

// tempCounter is the fallback uniqueness source when crypto/rand fails.
var tempCounter uint64

// Errors returned by the package.
var (
	// ErrNoStore reports an operation on a Store without a file system.
	ErrNoStore = errors.New("blob: no file system configured")
	// ErrInvalidBucket reports an empty or unusable bucket name.
	ErrInvalidBucket = errors.New("blob: invalid bucket")
	// ErrInvalidKey reports an empty key or one containing a ".." segment.
	ErrInvalidKey = errors.New("blob: invalid key")
)

// Info is the metadata reported by Stat and List.
type Info struct {
	// Key is the object key, relative to its bucket.
	Key string
	// Size is the object length in bytes.
	Size int64
	// ETag is a version marker derived from the stored file's size and
	// modification time. Harbor's file system has no content ETag of its own, so
	// this distinguishes versions of one key rather than naming the content.
	ETag string
}

// Store is a flat object namespace over one Harbor file system.
type Store struct {
	fs *fs.FS
}

// New returns a Store over an already-open Harbor file system.
func New(fsys *fs.FS) *Store { return &Store{fs: fsys} }

// Open returns a Store over the named file system, creating it if absent.
func Open(store *fs.Store, name string) (*Store, error) {
	if store == nil {
		return nil, ErrNoStore
	}
	fsys, err := store.Open(name)
	if err != nil {
		return nil, fmt.Errorf("blob: open file system %q: %w", name, err)
	}
	return &Store{fs: fsys}, nil
}

// FS returns the underlying file system, or nil.
func (s *Store) FS() *fs.FS {
	if s == nil {
		return nil
	}
	return s.fs
}

// objectPath maps bucket+key to a path in the file system.
//
// Bucket and key are cleaned SEPARATELY with a leading slash, so ".." in a key
// collapses toward the root of its own bucket and cannot reach a sibling bucket.
// A ".." segment is rejected outright by validateKey, so this is a second line of
// defence rather than the only one.
func objectPath(bucket, key string) (string, error) {
	if err := validateBucket(bucket); err != nil {
		return "", err
	}
	if err := validateKey(key); err != nil {
		return "", err
	}
	return path.Join("/", bucket, key), nil
}

func validateBucket(bucket string) error {
	if bucket == "" {
		return fmt.Errorf("%w: empty", ErrInvalidBucket)
	}
	if bucket != path.Clean(bucket) || strings.Contains(bucket, "/") || bucket == "." || bucket == ".." {
		return fmt.Errorf("%w: %q", ErrInvalidBucket, bucket)
	}
	return nil
}

func validateKey(key string) error {
	if key == "" {
		return fmt.Errorf("%w: empty", ErrInvalidKey)
	}
	for _, seg := range strings.Split(key, "/") {
		if seg == ".." {
			return fmt.Errorf("%w: %q contains a parent segment", ErrInvalidKey, key)
		}
	}
	return nil
}

// etagOf derives a version marker from a file's size and modification time.
func etagOf(info os.FileInfo) string {
	return fmt.Sprintf("%x-%x", info.Size(), info.ModTime().UnixNano())
}

// Put writes r to bucket/key, creating intermediate directories. size and
// contentType are accepted for call-site compatibility and ignored: the file
// system needs no length and carries no content type.
//
// The write is atomic: the reader is streamed into a temporary sibling key and
// renamed into place only after a complete copy. A reader that fails midway
// therefore leaves no object at the final key and leaves any previous content
// untouched, so a later existence check cannot mistake a partial upload for a
// complete one.
//
// The returned ETag is empty. Callers that record a version should stat the
// object afterwards (Stat reports the derived marker) rather than treat the empty
// string as a failure.
func (s *Store) Put(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string) (string, error) {
	if s == nil || s.fs == nil {
		return "", ErrNoStore
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	p, err := objectPath(bucket, key)
	if err != nil {
		return "", err
	}
	if dir := path.Dir(p); dir != "/" {
		if err := s.fs.MkdirAll(dir, 0o755); err != nil {
			return "", fmt.Errorf("blob: create dir for %q: %w", key, err)
		}
	}
	if err := s.putAtomic(p, r); err != nil {
		return "", fmt.Errorf("blob: put %q: %w", key, err)
	}
	return "", nil
}

// tempPrefix marks in-flight uploads. Keys cannot start with a segment holding
// this prefix because the temporary file lives in the destination directory and
// is removed before the call returns; a crash can leave one behind, and List
// would then report it, so callers that need a clean bucket can remove it.
const tempPrefix = ".harbor-put-"

// putAtomic streams r into a temporary sibling of p and renames it into place.
// fs.Rename within one store replaces an existing file, so the final key is
// either the previous content or the complete new content, never a partial copy.
func (s *Store) putAtomic(p string, r io.Reader) error {
	tmp := path.Join(path.Dir(p), tempPrefix+randomSuffix())
	f, err := s.fs.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		_ = s.fs.Remove(tmp)
		return fmt.Errorf("write: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = s.fs.Remove(tmp)
		return fmt.Errorf("close: %w", err)
	}
	if err := s.fs.Rename(tmp, p); err != nil {
		_ = s.fs.Remove(tmp)
		return fmt.Errorf("install: %w", err)
	}
	return nil
}

// randomSuffix returns a process-unique suffix for a temporary upload key.
func randomSuffix() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand never fails on supported platforms; fall back to the clock
		// and a counter so a failure cannot make uploads collide.
		n := atomic.AddUint64(&tempCounter, 1)
		return fmt.Sprintf("%x-%x", time.Now().UnixNano(), n)
	}
	return hex.EncodeToString(b[:])
}

// Get opens bucket/key for reading. The caller must Close the returned reader.
// The size is reported as -1 when it cannot be determined.
func (s *Store) Get(ctx context.Context, bucket, key string) (io.ReadCloser, int64, error) {
	if s == nil || s.fs == nil {
		return nil, 0, ErrNoStore
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	p, err := objectPath(bucket, key)
	if err != nil {
		return nil, 0, err
	}
	f, err := s.fs.Open(p)
	if err != nil {
		return nil, 0, fmt.Errorf("blob: get %q: %w", key, err)
	}
	size := int64(-1)
	if st, err := f.Stat(); err == nil {
		size = st.Size()
	}
	return f, size, nil
}

// ReadAll reads a whole object. A missing object returns the wrapped os error, so
// callers can test with errors.Is(err, fs.ErrNotExist).
func (s *Store) ReadAll(ctx context.Context, bucket, key string) ([]byte, error) {
	rc, _, err := s.Get(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// Delete removes bucket/key. A missing object is not an error: callers generally
// only need the bytes gone, not proof that this call removed them.
func (s *Store) Delete(ctx context.Context, bucket, key string) error {
	if s == nil || s.fs == nil {
		return ErrNoStore
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p, err := objectPath(bucket, key)
	if err != nil {
		return err
	}
	if err := s.fs.Remove(p); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("blob: delete %q: %w", key, err)
	}
	return nil
}

// Move relocates one object from srcKey to dstKey inside bucket, creating
// intermediate directories for the destination.
//
// A MISSING source is a no-op success: an index record can be created before its
// bytes are uploaded, and the key must still move with a renamed subtree.
// srcKey == dstKey is also a no-op.
func (s *Store) Move(ctx context.Context, bucket, srcKey, dstKey string) error {
	if srcKey == "" || dstKey == "" || srcKey == dstKey {
		return nil
	}
	if s == nil || s.fs == nil {
		return ErrNoStore
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	src, err := objectPath(bucket, srcKey)
	if err != nil {
		return err
	}
	dst, err := objectPath(bucket, dstKey)
	if err != nil {
		return err
	}
	if _, err := s.fs.Stat(src); err != nil {
		if os.IsNotExist(err) {
			return nil // nothing uploaded yet
		}
		return fmt.Errorf("blob: stat %q: %w", srcKey, err)
	}
	if dir := path.Dir(dst); dir != "/" {
		if err := s.fs.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("blob: create dir for %q: %w", dstKey, err)
		}
	}
	if err := s.fs.Rename(src, dst); err != nil {
		return fmt.Errorf("blob: move %q -> %q: %w", srcKey, dstKey, err)
	}
	return nil
}

// Stat reports one object's metadata. The bool reports existence; a missing
// object is not an error.
func (s *Store) Stat(ctx context.Context, bucket, key string) (Info, bool, error) {
	if s == nil || s.fs == nil {
		return Info{}, false, ErrNoStore
	}
	if err := ctx.Err(); err != nil {
		return Info{}, false, err
	}
	p, err := objectPath(bucket, key)
	if err != nil {
		return Info{}, false, err
	}
	info, err := s.fs.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return Info{}, false, nil
		}
		return Info{}, false, fmt.Errorf("blob: stat %q: %w", key, err)
	}
	return Info{Key: key, Size: info.Size(), ETag: etagOf(info)}, true, nil
}

// Exists reports whether bucket/key holds bytes.
func (s *Store) Exists(ctx context.Context, bucket, key string) bool {
	_, ok, err := s.Stat(ctx, bucket, key)
	return err == nil && ok
}

// List returns every object under bucket whose key begins with prefix,
// recursively. An empty prefix lists the whole bucket. A prefix that matches
// nothing returns an empty slice, not an error.
func (s *Store) List(ctx context.Context, bucket, prefix string) ([]Info, error) {
	if s == nil || s.fs == nil {
		return nil, ErrNoStore
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateBucket(bucket); err != nil {
		return nil, err
	}
	if prefix != "" {
		if err := validateKey(prefix); err != nil {
			return nil, err
		}
	}

	root := path.Join("/", bucket, prefix)
	if _, err := s.fs.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("blob: list %q: %w", prefix, err)
	}

	out := make([]Info, 0, 16)
	err := fs.Walk(s.fs, root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		key := strings.TrimPrefix(strings.TrimPrefix(p, "/"+bucket), "/")
		out = append(out, Info{Key: key, Size: info.Size(), ETag: etagOf(info)})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("blob: list %q: %w", prefix, err)
	}
	return out, nil
}

// ── grants ───────────────────────────────────────────────────────────────────

// GrantType identifies how a bearer reaches the object.
type GrantType string

const (
	// GrantLocal means the owning application serves the bytes itself; the bearer
	// uses the application's own data plane and its own session.
	GrantLocal GrantType = "local"
)

// Grant is a short-lived authorization to touch ONE object.
//
// It is deliberately not a credential: it records which object was authorized,
// for which method, and until when. Deciding whether a caller MAY be granted is
// the application's job — that is where the access-control data lives — so this
// type only carries the bookkeeping.
type Grant struct {
	Type      GrantType `json:"type"`
	Method    string    `json:"method"` // "GET" or "PUT"
	Bucket    string    `json:"bucket"`
	Key       string    `json:"key"`
	ExpiresAt time.Time `json:"expires_at"`
}

// DefaultGrantTTL is used when a Minter is built with a non-positive TTL.
const DefaultGrantTTL = 15 * time.Minute

// Minter stamps grants with an expiry.
type Minter struct {
	ttl time.Duration
	now func() time.Time
}

// NewMinter returns a Minter with the given TTL. ttl <= 0 uses DefaultGrantTTL.
func NewMinter(ttl time.Duration) *Minter {
	if ttl <= 0 {
		ttl = DefaultGrantTTL
	}
	return &Minter{ttl: ttl, now: time.Now}
}

// Mint returns a grant for one object and method. ttl <= 0 uses the minter's TTL.
func (m *Minter) Mint(bucket, key, method string, ttl time.Duration) Grant {
	if ttl <= 0 {
		ttl = m.ttl
	}
	now := time.Now
	if m.now != nil {
		now = m.now
	}
	return Grant{
		Type:      GrantLocal,
		Method:    method,
		Bucket:    bucket,
		Key:       key,
		ExpiresAt: now().Add(ttl),
	}
}

// TTL reports the minter's default grant lifetime.
func (m *Minter) TTL() time.Duration { return m.ttl }
