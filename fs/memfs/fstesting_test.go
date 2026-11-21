package memfs_test

import (
	"testing"

	"github.com/malivvan/storm/fs"
	"github.com/malivvan/storm/fs/memfs"
)

// TestMemFSSuite runs the standard fstesting suite against memfs.
func TestMemFSSuite(t *testing.T) {
	mfs, err := memfs.NewFS()
	if err != nil {
		t.Fatalf("failed to create memfs: %v", err)
	}

	suite := &fs.Suite{
		FS: mfs,
		Features: fs.Features{
			Symlinks:      true,
			HardLinks:     false,
			Permissions:   true,
			Timestamps:    true,
			CaseSensitive: true,
			AtomicRename:  true,
			SparseFiles:   false,
			LargeFiles:    true,
		},
	}

	suite.Run(t)
}
