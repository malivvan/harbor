package absfs

import (
	"os"
	"testing"
)

// TestExtendedFSMkdirAllOverFile guards F6: a file in the path must surface an
// error instead of silently doing nothing.
func TestExtendedFSMkdirAllOverFile(t *testing.T) {
	mock := newMockFiler()
	fs := ExtendFiler(mock)

	f, err := fs.OpenFile("/a", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("create /a: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := fs.MkdirAll("/a/b", 0755); err == nil {
		t.Fatal("MkdirAll over an existing file succeeded, want an error")
	}
}

// TestFileAdapterTruncateCannotShrink guards F7: truncating without native
// support must not leave the tail behind.
func TestFileAdapterTruncateCannotShrink(t *testing.T) {
	sf := &testSeekable{name: "x", data: []byte("0123456789")}
	f := ExtendSeekable(sf)

	if err := f.Truncate(4); err == nil {
		t.Fatal("shrinking without a native Truncate succeeded, want an error")
	}
	if string(sf.data) != "0123456789" {
		t.Fatalf("failed truncate modified the data: %q", sf.data)
	}

	// Growing appends zeros and keeps the existing bytes.
	if err := f.Truncate(12); err != nil {
		t.Fatalf("grow: %v", err)
	}
	if string(sf.data) != "0123456789\x00\x00" {
		t.Fatalf("grown data = %q", sf.data)
	}
}
