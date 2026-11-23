package fs

import (
	"errors"
	"os"
	"testing"
)

// failingContentFS wraps a real ContentFS and can be taken offline to make
// external content writes fail.
type failingContentFS struct {
	ContentFS
	offline bool
}

func (f *failingContentFS) WriteFile(name string, data []byte, perm os.FileMode) error {
	if f.offline {
		return errors.New("content store offline")
	}
	return f.ContentFS.WriteFile(name, data, perm)
}

// A failed external-content write must not commit a larger size into the
// inode: that would advertise bytes that never reached the store, and a later
// read would silently shrink the logical file to the content length.
func TestContentFSFailedWriteDoesNotGrowInode(t *testing.T) {
	content := &failingContentFS{ContentFS: newContentStorageFS(t).AsContentFS()}
	fsys, err := openFSWithContentForTest(t, tempFile(t), "", content)
	if err != nil {
		t.Fatal(err)
	}
	defer fsys.Close()

	writeStoreFile(t, fsys, "/f", "abc")

	// Take the external content store offline so the next content write fails.
	content.offline = true

	f, err := fsys.OpenFile("/f", os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open for append: %v", err)
	}
	if _, err := f.Write([]byte("ab")); err == nil {
		t.Fatal("Write succeeded although the external content store is offline")
	}
	_ = f.Close()

	info, err := fsys.Stat("/f")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() != 3 {
		t.Fatalf("inode size = %d after a failed content write, want 3", info.Size())
	}
}

// A content/metadata size mismatch must be reported rather than silently
// adopting the shorter content as the new file size.
func TestContentFSSizeMismatchIsReported(t *testing.T) {
	content := &failingContentFS{ContentFS: newContentStorageFS(t).AsContentFS()}
	fsys, err := openFSWithContentForTest(t, tempFile(t), "", content)
	if err != nil {
		t.Fatal(err)
	}
	defer fsys.Close()

	writeStoreFile(t, fsys, "/f", "abc")

	info, err := fsys.Stat("/f")
	if err != nil {
		t.Fatal(err)
	}
	ino := info.Sys().(*iNode).Ino

	// Simulate a torn write: grow the inode size behind the filesystem's back
	// while the content still holds only 3 bytes.
	node, err := fsys.loadInode(ino)
	if err != nil {
		t.Fatal(err)
	}
	node.Size = 5
	if _, err := fsys.saveInode(node); err != nil {
		t.Fatal(err)
	}

	f, err := fsys.Open("/f")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Read(make([]byte, 16)); err == nil {
		t.Fatal("reading a size/content mismatch must report an error")
	}

	// The logical size must not have been rewritten to the content length.
	info, err = fsys.Stat("/f")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 5 {
		t.Fatalf("inode size rewritten to %d, want the committed 5", info.Size())
	}
}
