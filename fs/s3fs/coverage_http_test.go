package s3fs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTTPClientAbortMultipartUpload(t *testing.T) {
	fsys, fake := newHTTPTestFS(t)
	ctx := context.Background()

	out, err := fsys.client.CreateMultipartUpload(ctx, &CreateMultipartUploadInput{
		Bucket: testBucket,
		Key:    "file",
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.UploadID)

	fake.mu.Lock()
	_, exists := fake.multipart[out.UploadID]
	fake.mu.Unlock()
	require.True(t, exists)

	_, err = fsys.client.AbortMultipartUpload(ctx, &AbortMultipartUploadInput{
		Bucket:   testBucket,
		Key:      "file",
		UploadID: out.UploadID,
	})
	require.NoError(t, err)

	fake.mu.Lock()
	_, exists = fake.multipart[out.UploadID]
	fake.mu.Unlock()
	require.False(t, exists)
}

func TestHTTPClientErrorResponses(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"xml", `<?xml version="1.0"?><Error><Code>InternalError</Code><Message>boom</Message></Error>`},
		{"plain", `plain failure`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()

			fsys, err := New(&Config{
				Bucket:    testBucket,
				Region:    "us-east-1",
				Endpoint:  srv.URL,
				AccessKey: testAccessKey,
				SecretKey: testSecretKey,
			})
			require.NoError(t, err)

			ctx := context.Background()
			_, err = fsys.client.GetObject(ctx, &GetObjectInput{Bucket: testBucket, Key: "k"})
			require.Error(t, err)
			_, err = fsys.client.PutObject(ctx, &PutObjectInput{Bucket: testBucket, Key: "k", Body: strings.NewReader("x")})
			require.Error(t, err)
			_, err = fsys.client.DeleteObject(ctx, &DeleteObjectInput{Bucket: testBucket, Key: "k"})
			require.Error(t, err)
			_, err = fsys.client.CopyObject(ctx, &CopyObjectInput{Bucket: testBucket, CopySource: "/b/k", Key: "k"})
			require.Error(t, err)
			_, err = fsys.client.HeadObject(ctx, &HeadObjectInput{Bucket: testBucket, Key: "k"})
			require.Error(t, err)
			_, err = fsys.client.ListObjectsV2(ctx, &ListObjectsV2Input{Bucket: testBucket})
			require.Error(t, err)
			_, err = fsys.client.CreateMultipartUpload(ctx, &CreateMultipartUploadInput{Bucket: testBucket, Key: "k"})
			require.Error(t, err)
			_, err = fsys.client.UploadPart(ctx, &UploadPartInput{Bucket: testBucket, Key: "k", UploadID: "u", PartNumber: 1, Body: strings.NewReader("x")})
			require.Error(t, err)
			_, err = fsys.client.CompleteMultipartUpload(ctx, &CompleteMultipartUploadInput{Bucket: testBucket, Key: "k", UploadID: "u"})
			require.Error(t, err)
			_, err = fsys.client.AbortMultipartUpload(ctx, &AbortMultipartUploadInput{Bucket: testBucket, Key: "k", UploadID: "u"})
			require.Error(t, err)
		})
	}
}

func TestSharedConfigPathFallback(t *testing.T) {
	t.Setenv("AWS_CONFIG_FILE", filepath.Join("custom", "config"))
	require.Equal(t, filepath.Join("custom", "config"), sharedConfigPath())

	t.Setenv("AWS_CONFIG_FILE", "")
	home := t.TempDir()
	t.Setenv("HOME", home)
	require.Equal(t, filepath.Join(home, ".aws", "config"), sharedConfigPath())
}

func TestMockErrorInjection(t *testing.T) {
	ctx := context.Background()
	injected := errors.New("injected")

	t.Run("head", func(t *testing.T) {
		fsys, mock := newTestFS()
		mock.HeadObjectErr = injected
		_, err := fsys.Stat("f")
		require.Error(t, err)
		_, err = fsys.OpenFile("f", os.O_CREATE|os.O_EXCL, 0644)
		require.Error(t, err)
		require.Error(t, fsys.Mkdir("d", 0755))
	})

	t.Run("get", func(t *testing.T) {
		fsys, mock := newTestFS()
		mock.GetObjectErr = injected
		_, err := fsys.ReadFile("f")
		require.Error(t, err)
		_, err = fsys.OpenFile("f", os.O_WRONLY, 0644)
		require.Error(t, err)
	})

	t.Run("put", func(t *testing.T) {
		fsys, mock := newTestFS()
		mock.PutObjectErr = injected
		require.Error(t, fsys.Mkdir("d", 0755))
		mock.PutTestObject("f", []byte("data"))
		require.Error(t, fsys.Truncate("f", 1))
		f, err := fsys.OpenFile("new", os.O_CREATE|os.O_WRONLY, 0644)
		require.NoError(t, err)
		_, err = f.Write([]byte("x"))
		if err == nil {
			err = f.Close()
		}
		require.Error(t, err)
	})

	t.Run("delete", func(t *testing.T) {
		fsys, mock := newTestFS()
		mock.PutTestObject("f", []byte("data"))
		mock.DeleteObjectErr = injected
		require.Error(t, fsys.Remove("f"))
		mock.PutTestObject("a", []byte("data"))
		require.Error(t, fsys.Rename("a", "b"))
	})

	t.Run("copy", func(t *testing.T) {
		fsys, mock := newTestFS()
		mock.PutTestObject("a", []byte("data"))
		mock.CopyObjectErr = injected
		require.Error(t, fsys.Rename("a", "b"))
	})

	t.Run("list", func(t *testing.T) {
		fsys, mock := newTestFS()
		mock.ListObjectsErr = injected
		_, err := fsys.ReadDir("dir")
		require.Error(t, err)
		_, err = fsys.Sub("dir")
		require.Error(t, err)
	})

	t.Run("missing", func(t *testing.T) {
		fsys, _ := newTestFS()
		_, err := fsys.ReadFile("missing")
		require.Error(t, err)
		_, err = fsys.OpenFile("missing", os.O_RDONLY, 0)
		require.Error(t, err)
		require.Error(t, fsys.Rename("missing", "x"))
	})

	_ = ctx
}

func TestS3FileReadDirBranches(t *testing.T) {
	fsys, mock := newTestFS()
	mock.PutTestObject("file", []byte("data"))
	mock.PutTestObject("dir/", nil)
	mock.PutTestObject("dir/a.txt", []byte("A"))
	mock.PutTestObject("dir/sub/", nil)
	mock.PutTestObject("dir/sub/b.txt", []byte("B"))

	// A regular file is not a directory.
	f, err := fsys.OpenFile("file", os.O_RDONLY, 0)
	require.NoError(t, err)
	_, err = f.ReadDir(-1)
	require.Error(t, err)
	require.NoError(t, f.Close())

	// Directory listing with n <= 0 and n > 0.
	d, err := fsys.OpenFile("dir/", os.O_RDONLY, 0)
	require.NoError(t, err)
	entries, err := d.ReadDir(-1)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	require.NoError(t, d.Close())

	d, err = fsys.OpenFile("dir/", os.O_RDONLY, 0)
	require.NoError(t, err)
	entries, err = d.ReadDir(1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.NoError(t, d.Close())

	d, err = fsys.OpenFile("dir/", os.O_RDONLY, 0)
	require.NoError(t, err)
	names, err := d.Readdirnames(-1)
	require.NoError(t, err)
	require.NotEmpty(t, names)
	require.NoError(t, d.Close())

	// Writing handles still enumerate their prefix.
	w, err := fsys.OpenFile("wdir/", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	entries, err = w.ReadDir(-1)
	require.NoError(t, err)
	require.Empty(t, entries)
	require.NoError(t, w.Close())
}

func TestFileSystemErrorBranches(t *testing.T) {
	fsys, mock := newTestFS()
	mock.PutTestObject("file", []byte("data"))
	mock.PutTestObject("dir/", nil)

	// Invalid paths are rejected by every operation.
	for _, name := range []string{"../escape", "a/../../b"} {
		_, err := fsys.OpenFile(name, os.O_RDONLY, 0)
		require.Error(t, err)
		require.Error(t, fsys.Mkdir(name, 0755))
		require.Error(t, fsys.Remove(name))
		require.Error(t, fsys.Rename(name, "x"))
		_, err = fsys.Stat(name)
		require.Error(t, err)
		_, err = fsys.ReadFile(name)
		require.Error(t, err)
	}

	// O_EXCL on an existing object.
	_, err := fsys.OpenFile("file", os.O_CREATE|os.O_EXCL, 0644)
	require.ErrorIs(t, err, os.ErrExist)

	// Writing to an existing directory marker is rejected.
	_, err = fsys.OpenFile("dir/", os.O_WRONLY, 0644)
	require.Error(t, err)

	// Mkdir over an existing file and an existing directory.
	require.ErrorIs(t, fsys.Mkdir("file", 0755), os.ErrExist)
	require.ErrorIs(t, fsys.Mkdir("dir", 0755), os.ErrExist)

	// Rename to the same path is a no-op; missing sources fail.
	require.NoError(t, fsys.Rename("file", "file"))
	require.Error(t, fsys.Rename("missing", "x"))

	// ReadFile on a directory fails.
	_, err = fsys.ReadFile("dir/")
	require.Error(t, err)
}
