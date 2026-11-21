package inode

import (
	"strings"
	"sync"
	"syscall"
	"testing"
)

// TestUnlinkAllLinkCounts verifies that clearing a directory releases both the
// "." and ".." link counts instead of leaking the ".." count.
func TestUnlinkAllLinkCounts(t *testing.T) {
	ino := new(Ino)
	root := ino.NewDir(0777)
	parent := ino.NewDir(0777)
	if err := root.Link("p", parent); err != nil {
		t.Fatal(err)
	}
	if err := parent.Link("..", root); err != nil {
		t.Fatal(err)
	}
	file := ino.New(0666)
	if err := parent.Link("f", file); err != nil {
		t.Fatal(err)
	}

	rootBefore := root.Nlink
	parentBefore := parent.Nlink

	parent.UnlinkAll()

	if len(parent.ReadDir()) != 0 {
		t.Fatalf("directory not cleared: %d entries", len(parent.ReadDir()))
	}
	if file.Nlink != 0 {
		t.Fatalf("file link count = %d, want 0", file.Nlink)
	}
	// The ".." reference from parent to root must be released.
	if root.Nlink != rootBefore-1 {
		t.Fatalf("root link count = %d, want %d (the .. reference leaked)", root.Nlink, rootBefore-1)
	}
	// The directory's own "." count is released; only root's "p" link to it
	// remains, so the count drops by exactly one.
	if parent.Nlink != parentBefore-1 {
		t.Fatalf("parent link count = %d, want %d", parent.Nlink, parentBefore-1)
	}
}

// TestWalkSkipsDotDotAndCycle verifies Walk skips "."/".." and does not recurse
// forever through an entry pointing at an ancestor.
func TestWalkSkipsDotDotAndCycle(t *testing.T) {
	ino := new(Ino)
	root := ino.NewDir(0777)
	child := ino.NewDir(0777)
	if err := root.Link("child", child); err != nil {
		t.Fatal(err)
	}
	if err := child.Link("..", root); err != nil {
		t.Fatal(err)
	}
	// A cycle: an entry inside child pointing back at the root.
	if err := child.Link("loop", root); err != nil {
		t.Fatal(err)
	}

	visited := map[string]bool{}
	err := Walk(root, "/", func(p string, n *Inode) error {
		visited[p] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for p := range visited {
		if strings.HasSuffix(p, "/.") || strings.HasSuffix(p, "/..") {
			t.Errorf("Walk visited dot entry %q", p)
		}
	}
	if !visited["/"] || !visited["/child"] {
		t.Errorf("Walk missed expected paths, visited=%v", visited)
	}
	if visited["/child/loop"] {
		t.Errorf("Walk followed cycle into ancestor")
	}
}

// TestRenameConcurrentNoCorruption stress-tests concurrent renames of a single
// file between two names. With correct locking the link count stays consistent
// and the inode remains reachable; the stale-node bug could corrupt the count.
func TestRenameConcurrentNoCorruption(t *testing.T) {
	ino := new(Ino)
	root := ino.NewDir(0777)
	file := ino.New(0666)
	if err := root.Link("a", file); err != nil {
		t.Fatal(err)
	}

	const workers = 8
	const iters = 300
	var wg sync.WaitGroup
	wg.Add(workers)
	for g := 0; g < workers; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				if err := root.Rename("/a", "/b"); err != nil &&
					err != syscall.EEXIST && err != syscall.ENOENT && err != syscall.ENOTDIR {
					t.Errorf("rename a->b: %v", err)
					return
				}
				if err := root.Rename("/b", "/a"); err != nil &&
					err != syscall.EEXIST && err != syscall.ENOENT && err != syscall.ENOTDIR {
					t.Errorf("rename b->a: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	if file.Nlink != 1 {
		t.Fatalf("file link count = %d, want 1 (corrupted by concurrent renames)", file.Nlink)
	}
	if _, err := root.Resolve("/a"); err != nil {
		if _, err2 := root.Resolve("/b"); err2 != nil {
			t.Fatalf("file unreachable after renames: %v / %v", err, err2)
		}
	}
}
