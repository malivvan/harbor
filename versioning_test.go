package harbor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/harbor/pkg/audit"
	"github.com/malivvan/harbor/store/file"
	"github.com/malivvan/harbor/store/record"
	"github.com/malivvan/harbor/store/series"
	"github.com/malivvan/harbor/store/vector"
)

// TestSingleVersionOnePolicy pins the first-release policy: every persisted
// format has exactly one version, number 1. A future format change must bump the
// relevant exported constant and ship a compatibility path, which makes this
// test fail until that change is deliberate and documented.
func TestSingleVersionOnePolicy(t *testing.T) {
	require.Equal(t, "1.0.0", record.Version)
	require.Equal(t, uint64(1), file.FormatVersion)
	require.Equal(t, uint64(1), series.FormatVersion)
	require.Equal(t, uint32(1), vector.CollectionFormatVersion)
	require.Equal(t, 1, audit.FormatVersion)
}
