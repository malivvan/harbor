package harbor

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/harbor/store/search"
	"github.com/malivvan/harbor/store/series"
	"github.com/malivvan/harbor/store/vector"
)

type rootedRow struct {
	ID   string `db:"id"`
	Name string `db:"index"`
}

// TestRootedViewsAreIsolatedAndPersist proves the variadic bucket path on the
// store accessors selects a rooted view instead of the conventional root, and
// that the view is flushed and reopened correctly.
func TestRootedViewsAreIsolatedAndPersist(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "rooted.db")
	s := mustOpen(t, path, "rooted-password")

	// Record: a custom root is separate from the conventional "db" root.
	require.NoError(t, s.Record("tenant", "records").Save(ctx, &rootedRow{ID: "r1", Name: "custom"}))
	require.NoError(t, s.Record().Save(ctx, &rootedRow{ID: "r2", Name: "default"}))

	var row rootedRow
	require.NoError(t, s.Record("tenant", "records").One(ctx, "ID", "r1", &row))
	require.Equal(t, "custom", row.Name)
	require.Error(t, s.Record().One(ctx, "ID", "r1", &row), "the default root must not see a custom-root record")
	require.NoError(t, s.Record().One(ctx, "ID", "r2", &row))

	// File.
	tenantFS, err := s.File("tenant").Open("app")
	require.NoError(t, err)
	f, err := tenantFS.Create("/note.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("tenant"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	defaultFS, err := s.File().Open("app")
	require.NoError(t, err)
	_, err = defaultFS.ReadFile("/note.txt")
	require.Error(t, err, "the default root must not see a custom-root file")

	// Series.
	require.NoError(t, s.Series("tenant").Series("cpu").Insert(ctx, series.Point{Timestamp: 1_000, Value: 7}))
	defaultPoints, err := s.Series().Series("cpu").Range(ctx, 0, 10_000)
	require.NoError(t, err)
	require.Empty(t, defaultPoints)
	tenantPoints, err := s.Series("tenant").Series("cpu").Range(ctx, 0, 10_000)
	require.NoError(t, err)
	require.Len(t, tenantPoints, 1)

	// Vector.
	col, err := s.Vector("tenant").NewCollection("docs", 2, vector.Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]vector.PointStruct{{ID: "p1", Vector: []float32{1, 0}}}))
	defaultHits, err := s.Vector().NewCollection("docs", 2, vector.Cosine, false)
	require.NoError(t, err)
	require.Zero(t, defaultHits.Count())
	require.Equal(t, 1, col.Count())

	// Search.
	idx, err := s.Search("tenant").Open("docs", search.Config{})
	require.NoError(t, err)
	require.NoError(t, idx.Upsert(ctx, "d1", "tenant text"))
	defaultIdx, err := s.Search().Open("docs", search.Config{})
	require.NoError(t, err)
	defaultCount, err := defaultIdx.Count(ctx)
	require.NoError(t, err)
	require.Zero(t, defaultCount)

	// The base Flush drains vector views too.
	require.NoError(t, s.Vector().Flush())
	require.NoError(t, s.Close())

	// Reopen: custom roots persisted and remain separate from the defaults.
	reopened := mustOpen(t, path, "rooted-password")
	defer mustClose(t, reopened)

	require.NoError(t, reopened.Record("tenant", "records").One(ctx, "ID", "r1", &row))
	require.Equal(t, "custom", row.Name)
	require.NoError(t, reopened.Record().One(ctx, "ID", "r2", &row))

	reopenedCol, err := reopened.Vector("tenant").NewCollection("docs", 2, vector.Cosine, false)
	require.NoError(t, err)
	require.Equal(t, 1, reopenedCol.Count())

	reopenedIdx, err := reopened.Search("tenant").Open("docs", search.Config{})
	require.NoError(t, err)
	count, err := reopenedIdx.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

// TestAccessorPathsDoNotChangeDefaults keeps the no-argument accessors on the
// legacy on-disk roots so existing databases stay readable.
func TestAccessorPathsDoNotChangeDefaults(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "defaults.db")
	s := mustOpen(t, path, "default-password")
	require.NoError(t, s.Record().Save(ctx, &rootedRow{ID: "d1"}))
	require.NoError(t, s.Close())

	reopened := mustOpen(t, path, "default-password")
	defer mustClose(t, reopened)
	var row rootedRow
	require.NoError(t, reopened.Record().One(ctx, "ID", "d1", &row))
}
