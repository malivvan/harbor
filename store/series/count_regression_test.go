package series

import (
	"context"
	"testing"

	hstore "github.com/malivvan/tempest/store"
	"github.com/stretchr/testify/require"
)

// A series created by plain Insert has no metadata record, so its persisted
// Count reads as zero. DeleteRange must seed the count from the data instead
// of persisting a false zero while points remain.
func TestDeleteRangeSeedsCountForMetadataLessSeries(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	se := store.Series("cpu")

	for i := int64(1); i <= 10; i++ {
		require.NoError(t, se.Insert(ctx, Point{Timestamp: i, Value: float64(i)}))
	}

	// A delete that removes nothing must not create a Count-0 metadata record.
	deleted, err := se.DeleteRange(ctx, 100, 200)
	require.NoError(t, err)
	require.Zero(t, deleted)

	require.NoError(t, se.Insert(ctx, Point{Timestamp: 11, Value: 11}))
	count, err := se.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 11, count, "DeleteRange hid the existing points from Count")

	// A partial delete must subtract from the seeded count.
	deleted, err = se.DeleteRange(ctx, 1, 5)
	require.NoError(t, err)
	require.Equal(t, 5, deleted)
	count, err = se.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 6, count)
}

// The block path has the same count bookkeeping.
func TestDeleteRangeSeedsCountForBlockSeries(t *testing.T) {
	store := openBlockStore(t, []byte(hstore.SERIES), 7, 10)
	ctx := context.Background()
	se := store.Series("cpu")

	for i := int64(1); i <= 10; i++ {
		require.NoError(t, se.Insert(ctx, Point{Timestamp: i, Value: float64(i)}))
	}

	deleted, err := se.DeleteRange(ctx, 100, 200)
	require.NoError(t, err)
	require.Zero(t, deleted)

	require.NoError(t, se.Insert(ctx, Point{Timestamp: 11, Value: 11}))
	count, err := se.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 11, count)
}

// Per-series EnforceRetention must persist the pruned count; otherwise Count
// keeps over-reporting and every write scans the series again.
func TestSeriesEnforceRetentionPersistsCount(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	se := store.Series("cpu")

	for i := int64(1); i <= 10; i++ {
		require.NoError(t, se.Insert(ctx, Point{Timestamp: i, Value: float64(i)}))
	}
	// Setting the policy seeds Count from the data.
	require.NoError(t, se.SetRetention(ctx, Retention{MaxPoints: 3}))

	removed, err := se.EnforceRetention(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, 7, removed)

	count, err := se.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, count)

	// A second run has nothing left to prune.
	removed, err = se.EnforceRetention(ctx, 0)
	require.NoError(t, err)
	require.Zero(t, removed)
}
