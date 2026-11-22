package absfs

import (
	"os"
	"testing"
)

func TestParseFileModeRejectsLongInput(t *testing.T) {
	if _, err := ParseFileMode("drwxr-xr-xEXTRA"); err == nil {
		t.Fatal("ParseFileMode accepted an over-long mode string")
	}
}

func TestExtendedFSTruncateResizes(t *testing.T) {
	mock := newMockFiler()
	fs := ExtendFiler(mock)

	f, err := fs.OpenFile("/t", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := f.Write([]byte("0123456789")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := fs.Truncate("/t", 3); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	data, err := fs.ReadFile("/t")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(data) != "012" {
		t.Fatalf("content = %q, want 012", data)
	}
}
