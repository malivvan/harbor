package s3fs

import (
	"io"
	"os"
	"testing"
)

// TestFileSeekThenWriteOverwrites verifies that a Seek followed by Write
// overwrites at the seek offset instead of appending at end-of-file.
func TestFileSeekThenWriteOverwrites(t *testing.T) {
	fs, mock := newTestFS()
	mock.PutTestObject("seekwrite.txt", []byte("hello"))

	f, err := fs.OpenFile("seekwrite.txt", os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	if _, err := f.Write([]byte("J")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, _ := mock.GetTestObject("seekwrite.txt")
	if string(data) != "Jello" {
		t.Fatalf("content = %q, want %q", string(data), "Jello")
	}
}

// TestFileSeekThenRead verifies that a Seek followed by Read reads from the
// seek offset instead of from the beginning.
func TestFileSeekThenRead(t *testing.T) {
	fs, mock := newTestFS()
	mock.PutTestObject("seekread.txt", []byte("0123456789"))

	f, err := fs.OpenFile("seekread.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	if _, err := f.Seek(6, io.SeekStart); err != nil {
		t.Fatalf("Seek: %v", err)
	}

	buf := make([]byte, 10)
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read: %v", err)
	}
	if string(buf[:n]) != "6789" {
		t.Fatalf("read = %q, want %q", string(buf[:n]), "6789")
	}
}

// TestWriteAtNegativeOffset verifies negative offsets are rejected, not panic.
func TestWriteAtNegativeOffset(t *testing.T) {
	fs, _ := newTestFS()
	f, err := fs.OpenFile("neg.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.WriteAt([]byte("x"), -1); err != os.ErrInvalid {
		t.Fatalf("WriteAt(-1) = %v, want os.ErrInvalid", err)
	}
}

// TestTruncateNegativeSize verifies negative truncate sizes are rejected.
func TestTruncateNegativeSize(t *testing.T) {
	fs, _ := newTestFS()
	f, err := fs.OpenFile("neg2.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if err := f.Truncate(-1); err != os.ErrInvalid {
		t.Fatalf("Truncate(-1) = %v, want os.ErrInvalid", err)
	}
}
