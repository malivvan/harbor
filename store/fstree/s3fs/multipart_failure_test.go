package s3fs

import (
	"errors"
	"os"
	"testing"
)

// A failed part upload used to slice the already-uploaded prefix out of the
// buffer and forget it; Close then stored only the remainder as the whole
// object and returned success. The failure must now surface and the object
// must not be silently truncated.
func TestMultipartUploadFailureDoesNotTruncate(t *testing.T) {
	mock := NewMockS3Client()
	fs := NewWithClient(mock, "test-bucket")
	fs.partSize = 1 << 20

	f, err := fs.OpenFile("big.bin", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	mock.UploadPartErr = errors.New("upload part failed")
	if n, err := f.Write(make([]byte, 6<<20)); err == nil {
		t.Fatalf("Write succeeded (n=%d) despite a failing UploadPart", n)
	}

	if err := f.Close(); err == nil {
		t.Fatal("Close succeeded after a failed part upload; a truncated object may have been stored")
	}

	if stored, ok := mock.GetTestObject("big.bin"); ok {
		t.Fatalf("object was stored after a failed multipart upload (%d bytes)", len(stored))
	}

	// Once poisoned, further writes report the original failure rather than
	// silently buffering more data.
	if _, err := f.Write([]byte("more")); err == nil {
		t.Fatal("Write after a failed upload must fail")
	}
}

// The happy path still uploads every part and completes the upload.
func TestMultipartUploadStillWorks(t *testing.T) {
	mock := NewMockS3Client()
	fs := NewWithClient(mock, "test-bucket")
	fs.partSize = 1 << 20

	f, err := fs.OpenFile("ok.bin", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	payload := make([]byte, 3<<20)
	payload[0] = 7
	if n, err := f.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write = %d, %v", n, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	stored, ok := mock.GetTestObject("ok.bin")
	if !ok || len(stored) != len(payload) {
		t.Fatalf("stored object = %d bytes, ok=%v; want %d", len(stored), ok, len(payload))
	}
}
