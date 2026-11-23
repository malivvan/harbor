package harbor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/harbor/db"
	"github.com/malivvan/harbor/fs"
	"github.com/malivvan/harbor/pkg/audit"
	"github.com/malivvan/harbor/ts"
	"github.com/malivvan/harbor/vec"
)

// TestSingleVersionOnePolicy pins the first-release policy: every persisted
// format has exactly one version, number 1. A future format change must bump the
// relevant exported constant and ship a compatibility path, which makes this
// test fail until that change is deliberate and documented.
func TestSingleVersionOnePolicy(t *testing.T) {
	require.Equal(t, "1.0.0", db.Version)
	require.Equal(t, uint64(1), fs.FormatVersion)
	require.Equal(t, uint64(1), ts.FormatVersion)
	require.Equal(t, uint32(1), vec.CollectionFormatVersion)
	require.Equal(t, 1, audit.FormatVersion)
}
