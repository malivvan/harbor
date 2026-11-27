package object_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/tempest"
	"github.com/malivvan/tempest/internal/bucket"
	"github.com/malivvan/tempest/store/object"
)

func openDirect(t *testing.T, cfg object.Config) (*object.Store, *bolt.DB) {
	t.Helper()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "object.db"), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })
	cfg.Root = [][]byte{[]byte(bucket.OBJECT)}
	s, err := object.Open(context.Background(), bdb, cfg)
	require.NoError(t, err)
	return s, bdb
}

func failingReader(limit int) io.Reader {
	return &failReader{limit: limit}
}

type failReader struct{ sent, limit int }

func (r *failReader) Read(p []byte) (int, error) {
	if r.sent >= r.limit {
		return 0, errors.New("stream failed")
	}
	n := len(p)
	if remaining := r.limit - r.sent; n > remaining {
		n = remaining
	}
	for i := 0; i < n; i++ {
		p[i] = 'x'
	}
	r.sent += n
	return n, nil
}

func TestPutGetListDeleteMove(t *testing.T) {
	ctx := context.Background()
	s, _ := openDirect(t, object.Config{})

	_, err := s.Put(ctx, "tenant-a", "drive/notes.txt", strings.NewReader("hello world"), 11, "text/plain")
	require.NoError(t, err)

	rc, size, err := s.Get(ctx, "tenant-a", "drive/notes.txt")
	require.NoError(t, err)
	body, err := io.ReadAll(rc)
	require.NoError(t, rc.Close())
	require.NoError(t, err)
	require.Equal(t, "hello world", string(body))
	require.Equal(t, int64(11), size)

	info, ok, err := s.Stat(ctx, "tenant-a", "drive/notes.txt")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(11), info.Size)
	require.Len(t, info.ETag, 64)

	_, err = s.Put(ctx, "tenant-a", "a/b/c.txt", strings.NewReader("c"), 1, "")
	require.NoError(t, err)
	list, err := s.List(ctx, "tenant-a", "a/")
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, "a/b/c.txt", list[0].Key)

	require.NoError(t, s.Move(ctx, "tenant-a", "drive/notes.txt", "moved.txt"))
	require.False(t, s.Exists(ctx, "tenant-a", "drive/notes.txt"))
	require.True(t, s.Exists(ctx, "tenant-a", "moved.txt"))

	require.NoError(t, s.Delete(ctx, "tenant-a", "moved.txt"))
	require.NoError(t, s.Delete(ctx, "tenant-a", "moved.txt"))
	require.False(t, s.Exists(ctx, "tenant-a", "moved.txt"))

	// A missing source is a no-op, as in the blob API.
	require.NoError(t, s.Move(ctx, "tenant-a", "missing", "target"))
	// Stat of a missing object is not an error.
	_, ok, err = s.Stat(ctx, "tenant-a", "missing")
	require.NoError(t, err)
	require.False(t, ok)
	if _, _, err := s.Get(ctx, "tenant-a", "missing"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Get(missing) err = %v, want os.ErrNotExist", err)
	}
}

func TestPutIsAtomicOnReaderFailure(t *testing.T) {
	ctx := context.Background()
	s, _ := openDirect(t, object.Config{})

	_, err := s.Put(ctx, "b", "obj", strings.NewReader("original"), 8, "")
	require.NoError(t, err)
	_, err = s.Put(ctx, "b", "obj", failingReader(1024), 0, "")
	require.Error(t, err)
	got, err := s.ReadAll(ctx, "b", "obj")
	require.NoError(t, err)
	require.Equal(t, "original", string(got))

	_, err = s.Put(ctx, "b", "fresh", failingReader(1024), 0, "")
	require.Error(t, err)
	_, ok, err := s.Stat(ctx, "b", "fresh")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestContainment(t *testing.T) {
	ctx := context.Background()
	s, _ := openDirect(t, object.Config{})
	_, err := s.Put(ctx, "tenant-b", "secret.txt", strings.NewReader("SECRET"), 6, "")
	require.NoError(t, err)

	for _, key := range []string{"../tenant-b/secret.txt", "a/../../tenant-b/secret.txt", "..", "a/.."} {
		if _, err := s.Put(ctx, "tenant-a", key, strings.NewReader("x"), 1, ""); !errors.Is(err, object.ErrInvalidKey) {
			t.Errorf("Put(%q) err = %v, want ErrInvalidKey", key, err)
		}
		if _, _, err := s.Get(ctx, "tenant-a", key); !errors.Is(err, object.ErrInvalidKey) {
			t.Errorf("Get(%q) err = %v, want ErrInvalidKey", key, err)
		}
	}
	for _, bucket := range []string{"", "a/b", ".", "..", "__meta"} {
		if _, err := s.Put(ctx, bucket, "k", strings.NewReader("x"), 1, ""); !errors.Is(err, object.ErrInvalidBucket) {
			t.Errorf("Put bucket %q err = %v, want ErrInvalidBucket", bucket, err)
		}
	}
	got, err := s.ReadAll(ctx, "tenant-b", "secret.txt")
	require.NoError(t, err)
	require.Equal(t, "SECRET", string(got))
}

func TestPersistence(t *testing.T) {
	ctx := context.Background()
	s, bdb := openDirect(t, object.Config{})
	_, err := s.Put(ctx, "b", "a/k", strings.NewReader("payload"), 7, "")
	require.NoError(t, err)

	reopened, err := object.Open(ctx, bdb, object.Config{Root: [][]byte{[]byte(bucket.OBJECT)}})
	require.NoError(t, err)
	got, err := reopened.ReadAll(ctx, "b", "a/k")
	require.NoError(t, err)
	require.Equal(t, "payload", string(got))
}

func TestIsolationFromFileStore(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tempest.db")
	store, err := tempest.Open(ctx, path, 0o600, &tempest.Options{Password: "object-password", Plaintext: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	fsys, err := store.Fstree().Open("objects")
	require.NoError(t, err)
	require.NoError(t, fsys.MkdirAll("/bucket", 0o755))
	f, err := fsys.Create("/bucket/file.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("file-backed"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// The object store does not see the file-store object, and vice versa.
	_, err = store.Objects().ReadAll(ctx, "bucket", "file.txt")
	require.Error(t, err)
	_, err = store.Objects().Put(ctx, "bucket", "object.txt", strings.NewReader("object-backed"), 13, "")
	require.NoError(t, err)
	if _, err := fsys.ReadFile("/bucket/object.txt"); err == nil {
		t.Fatal("the fstree store sees an object-store key")
	}

	// The object store lives under its own root.
	require.NoError(t, store.View(func(tx *bolt.Tx) error {
		require.NotNil(t, tx.Bucket([]byte(bucket.OBJECT)))
		return nil
	}))
}

func TestRootStoreAccessAndTransactions(t *testing.T) {
	ctx := context.Background()
	store, err := tempest.Open(ctx, filepath.Join(t.TempDir(), "tempest.db"), 0o600, &tempest.Options{
		Password:   "object-password",
		KDFTime:    1,
		KDFMemory:  8 * tempest.MiB,
		KDFThreads: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	_, err = store.Objects().Put(ctx, "b", "committed", strings.NewReader("yes"), 3, "")
	require.NoError(t, err)
	body, err := store.Objects().ReadAll(ctx, "b", "committed")
	require.NoError(t, err)
	require.Equal(t, "yes", string(body))

	abort := errors.New("abort")
	err = store.WriteTransaction(ctx, func(tx *tempest.Tx) error {
		if _, err := tx.Objects().Put(ctx, "b", "aborted", strings.NewReader("no"), 2, ""); err != nil {
			return err
		}
		return abort
	})
	require.ErrorIs(t, err, abort)
	if _, err := store.Objects().ReadAll(ctx, "b", "aborted"); err == nil {
		t.Fatal("an aborted object write is visible")
	}

	// Rooted views are isolated.
	tenant := store.Objects("tenant")
	require.NotNil(t, tenant)
	// The rooted accessor returns a store; buckets remain per-call.
	_, err = tenant.Put(ctx, "b", "k", strings.NewReader("tenant"), 6, "")
	require.NoError(t, err)
	if _, err := store.Objects().ReadAll(ctx, "b", "k"); err == nil {
		t.Fatal("a rooted view leaked into the default root")
	}
}

func TestMigrateFromFile(t *testing.T) {
	ctx := context.Background()
	store, err := tempest.Open(ctx, filepath.Join(t.TempDir(), "tempest.db"), 0o600, &tempest.Options{
		Password:   "object-password",
		KDFTime:    1,
		KDFMemory:  8 * tempest.MiB,
		KDFThreads: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	fsys, err := store.Fstree().Open("objects")
	require.NoError(t, err)
	require.NoError(t, fsys.MkdirAll("/bucket/dir", 0o755))
	for name, body := range map[string]string{
		"/bucket/a.txt":     "alpha",
		"/bucket/dir/b.txt": "beta",
	} {
		f, err := fsys.Create(name)
		require.NoError(t, err)
		_, err = f.Write([]byte(body))
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	require.NoError(t, store.Objects().MigrateFromFile(ctx, fsys, "bucket"))
	got, err := store.Objects().ReadAll(ctx, "bucket", "a.txt")
	require.NoError(t, err)
	require.Equal(t, "alpha", string(got))
	got, err = store.Objects().ReadAll(ctx, "bucket", "dir/b.txt")
	require.NoError(t, err)
	require.Equal(t, "beta", string(got))

	// The source is untouched.
	body, err := fsys.ReadFile("/bucket/a.txt")
	require.NoError(t, err)
	require.Equal(t, "alpha", string(body))
}

func TestEncryptionAndVersion(t *testing.T) {
	ctx := context.Background()
	secret := []byte("0123456789abcdef0123456789abcdef")
	s, bdb := openDirect(t, object.Config{Secret: secret, EncryptValues: true})
	_, err := s.Put(ctx, "b", "k", strings.NewReader("secret"), 6, "")
	require.NoError(t, err)

	again, err := object.Open(ctx, bdb, object.Config{Root: [][]byte{[]byte(bucket.OBJECT)}, Secret: secret, EncryptValues: true})
	require.NoError(t, err)
	got, err := again.ReadAll(ctx, "b", "k")
	require.NoError(t, err)
	require.Equal(t, "secret", string(got))

	wrong := []byte("fedcba9876543210fedcba9876543210")
	_, err = object.Open(ctx, bdb, object.Config{Root: [][]byte{[]byte(bucket.OBJECT)}, Secret: wrong, EncryptValues: true})
	require.Error(t, err)
	_, err = object.Open(ctx, bdb, object.Config{Root: [][]byte{[]byte(bucket.OBJECT)}})
	require.ErrorIs(t, err, object.ErrWrongMode)

	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucket.OBJECT)).Put([]byte("__version__"), []byte{9, 0, 0, 0, 0, 0, 0, 0})
	}))
	_, err = object.Open(ctx, bdb, object.Config{Root: [][]byte{[]byte(bucket.OBJECT)}, Secret: secret, EncryptValues: true})
	require.ErrorIs(t, err, object.ErrIncompatibleVersion)
}

func TestGrants(t *testing.T) {
	m := object.NewMinter(time.Minute)
	g := m.Mint("bucket", "key", "GET", 0)
	require.Equal(t, object.GrantLocal, g.Type)
	require.Equal(t, "bucket", g.Bucket)
	require.Equal(t, "key", g.Key)
	require.True(t, g.ExpiresAt.After(time.Now()))
	require.Equal(t, object.DefaultGrantTTL, object.NewMinter(0).TTL())
}
