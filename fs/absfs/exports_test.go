package absfs

import (
	"testing"

	i "github.com/malivvan/storm/internal/absfs"
	"github.com/stretchr/testify/require"
)

// The public package only re-exports internal/absfs types and constants.
// These assertions fail to compile if an alias or value drifts.
var (
	_ File              = (*InvalidFile)(nil)
	_ FileSystem        = SymlinkFileSystem(nil)
	_ SymlinkFileSystem = (*SymlinkOverlay)(nil)
	_ Filer             = FileSystem(nil)
	_ SymLinker         = (*NoSymlinks)(nil)
)

func TestReExports(t *testing.T) {
	require.Equal(t, i.Separator, Separator)
	require.Equal(t, i.ListSeparator, ListSeparator)

	require.Equal(t, i.O_RDONLY, O_RDONLY)
	require.Equal(t, i.O_WRONLY, O_WRONLY)
	require.Equal(t, i.O_RDWR, O_RDWR)
	require.Equal(t, i.O_APPEND, O_APPEND)
	require.Equal(t, i.O_CREATE, O_CREATE)
	require.Equal(t, i.O_EXCL, O_EXCL)
	require.Equal(t, i.O_SYNC, O_SYNC)
	require.Equal(t, i.O_TRUNC, O_TRUNC)

	require.Equal(t, i.OS_READ, OS_READ)
	require.Equal(t, i.OS_WRITE, OS_WRITE)
	require.Equal(t, i.OS_EX, OS_EX)
	require.Equal(t, i.OS_USER_SHIFT, OS_USER_SHIFT)
	require.Equal(t, i.OS_GROUP_SHIFT, OS_GROUP_SHIFT)
	require.Equal(t, i.OS_OTH_SHIFT, OS_OTH_SHIFT)
	require.Equal(t, i.OS_ALL_RWX, OS_ALL_RWX)

	// The alias must remain the same type, not a distinct wrapper.
	var flags Flags = i.Flags(0)
	require.Equal(t, Flags(0), flags)
}
