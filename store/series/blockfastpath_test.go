package series

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestInsertBlockFastPaths exercises the single-block, sorted-batch and
// append-only fast paths, plus the multi-block grouping path and duplicate
// timestamp handling, and checks the stored points.
func TestInsertBlockFastPaths(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")
	store := openBlockStore(t, root, 51, 10)
	se := store.Series("cpu")

	// Single-block, already-sorted batch: no group map, no sort, append path.
	require.NoError(t, se.InsertMany(ctx, []Point{{1, 1}, {2, 2}, {3, 3}}))
	got, err := se.Range(ctx, 0, 9)
	require.NoError(t, err)
	require.Equal(t, []Point{{1, 1}, {2, 2}, {3, 3}}, got)

	// Append-only batch within the same block.
	require.NoError(t, se.InsertMany(ctx, []Point{{4, 4}, {5, 5}}))
	got, err = se.Range(ctx, 0, 9)
	require.NoError(t, err)
	require.Equal(t, []Point{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}}, got)

	// Out-of-order batch with duplicate timestamps: sorted, deduped, last wins.
	require.NoError(t, se.InsertMany(ctx, []Point{{3, 30}, {0, 0}, {2, 20}}))
	got, err = se.Range(ctx, 0, 9)
	require.NoError(t, err)
	require.Equal(t, []Point{{0, 0}, {1, 1}, {2, 20}, {3, 30}, {4, 4}, {5, 5}}, got)

	// Multi-block batch forces the grouping path.
	require.NoError(t, se.InsertMany(ctx, []Point{{15, 15}, {11, 11}, {21, 21}}))
	got, err = se.Range(ctx, 0, 30)
	require.NoError(t, err)
	require.Equal(t, []Point{
		{0, 0}, {1, 1}, {2, 20}, {3, 30}, {4, 4}, {5, 5},
		{11, 11}, {15, 15}, {21, 21},
	}, got)

	count, err := se.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 9, count)
}
