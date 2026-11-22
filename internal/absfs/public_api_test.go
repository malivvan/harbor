package absfs

import "testing"

// The public filesystem interfaces and helpers used to be re-exported from a
// separate wrapper package. They now live here, so these compile-time
// assertions guard the moved public surface.
var (
	_ File              = (*InvalidFile)(nil)
	_ FileSystem        = SymlinkFileSystem(nil)
	_ SymlinkFileSystem = (*SymlinkOverlay)(nil)
	_ Filer             = FileSystem(nil)
	_ SymLinker         = (*NoSymlinks)(nil)
	_ Seekable          = File(nil)
	_ UnSeekable        = Seekable(nil)
)

func TestPublicAPIExports(t *testing.T) {
	if Separator != '/' {
		t.Fatalf("Separator = %q, want '/'", Separator)
	}
	if O_RDONLY != 0 || O_WRONLY != 1 || O_RDWR != 2 {
		t.Fatalf("unexpected access flags: %d %d %d", O_RDONLY, O_WRONLY, O_RDWR)
	}

	// Keep the public helpers reachable; a rename breaks this test.
	helpers := []any{
		FilerToFS, ParseFileMode, ParseFlags,
		ExtendFiler, ExtendSeekable, ExtendSymlinkFiler, BlockSymlinks,
	}
	for i, helper := range helpers {
		if helper == nil {
			t.Fatalf("public helper %d is nil", i)
		}
	}

	var _ = Flags(0)
	var _ = InvalidFile{}
	var _ = NoSymlinks{}
	var _ = SymlinkOverlay{}
	_ = ListSeparator
}
