package s3fs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"
)

// TestRemoveNonEmptyDirectory guards F8: removing a directory with children
// must fail instead of orphaning them.
func TestRemoveNonEmptyDirectory(t *testing.T) {
	fs, mock := newTestFS()
	mock.PutTestObject("dir/", nil)
	mock.PutTestObject("dir/child.txt", []byte("data"))

	err := fs.Remove("dir")
	if !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("Remove(non-empty dir) = %v, want ENOTEMPTY", err)
	}
	if !mock.HasObject("dir/child.txt") || !mock.HasObject("dir/") {
		t.Fatal("Remove deleted objects from a non-empty directory")
	}
}

// TestRenameMovesDirectoryContents guards F8 for Rename.
func TestRenameMovesDirectoryContents(t *testing.T) {
	fs, mock := newTestFS()
	mock.PutTestObject("dir/", nil)
	mock.PutTestObject("dir/child.txt", []byte("data"))

	if err := fs.Rename("dir", "moved"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	for _, key := range []string{"moved/", "moved/child.txt"} {
		if !mock.HasObject(key) {
			t.Errorf("missing %q after rename", key)
		}
	}
	for _, key := range []string{"dir/", "dir/child.txt"} {
		if mock.HasObject(key) {
			t.Errorf("stale object %q after rename", key)
		}
	}
}

// TestOpenFileWriteSurfacesGetObjectError guards F3: an unreadable existing
// object must abort the open, so Close cannot overwrite it with a partial
// buffer.
func TestOpenFileWriteSurfacesGetObjectError(t *testing.T) {
	fs, mock := newTestFS()
	mock.PutTestObject("f.txt", []byte("existing"))
	mock.GetObjectErr = errors.New("access denied")

	f, err := fs.OpenFile("f.txt", os.O_WRONLY, 0644)
	if err == nil {
		if f != nil {
			_ = f.Close()
		}
		t.Fatal("OpenFile succeeded despite a GetObject error")
	}
	if !errors.Is(err, mock.GetObjectErr) {
		t.Fatalf("OpenFile error = %v, want it to wrap the GetObject error", err)
	}
	if len(mock.PutObjectCalls) != 0 {
		t.Fatalf("OpenFile wrote %v; it must not overwrite the object", mock.PutObjectCalls)
	}
}

// TestReadDirSorted guards F18.
func TestReadDirSorted(t *testing.T) {
	fs, mock := newTestFS()
	mock.PutTestObject("a.txt", []byte("x"))
	mock.PutTestObject("a/", nil)

	entries, err := fs.ReadDir("/")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	if strings.Join(names, ",") != "a,a.txt" {
		t.Fatalf("ReadDir names = %v, want [a a.txt]", names)
	}
}

// trackingBody records whether it was closed.
type trackingBody struct {
	io.Reader
	closed bool
}

func (b *trackingBody) Close() error {
	b.closed = true
	return nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestGetObjectClosesBodyOnUnexpectedStatus guards F17.
func TestGetObjectClosesBodyOnUnexpectedStatus(t *testing.T) {
	var body *trackingBody
	client, err := newHTTPClient(&Config{
		Bucket:    "bucket",
		Region:    "us-east-1",
		Endpoint:  "https://s3.example.com",
		AccessKey: "AKIDEXAMPLE",
		SecretKey: "secret",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			body = &trackingBody{Reader: strings.NewReader("boom")}
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Status:     "500 Internal Server Error",
				Body:       body,
				Header:     make(http.Header),
			}, nil
		})},
	})
	if err != nil {
		t.Fatalf("newHTTPClient: %v", err)
	}

	if _, err := client.GetObject(context.Background(), &GetObjectInput{Bucket: "bucket", Key: "key"}); err == nil {
		t.Fatal("GetObject returned nil error for a 500")
	}
	if body == nil || !body.closed {
		t.Fatal("GetObject leaked the response body on an unexpected status")
	}
}
