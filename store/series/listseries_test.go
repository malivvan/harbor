package series

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestListSeries verifies that persisted series are enumerated from the store,
// including a series that has points but no metadata record, and that a fresh
// Store over the same handle (a reopen) still lists them even though its
// in-memory series set is empty.
func TestListSeries(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")
	store, db := openTestStore(t, root)

	// "cpu" has metadata (labels, retention) and one point.
	cpu := store.Series("cpu")
	require.NoError(t, cpu.Insert(ctx, Point{Timestamp: 1, Value: 1}))
	require.NoError(t, cpu.SetLabels(ctx, map[string]string{"host": "a"}))
	require.NoError(t, cpu.SetRetention(ctx, Retention{MaxPoints: 10}))

	// "mem" has a point but no metadata record (a plain Insert writes none).
	require.NoError(t, store.Series("mem").Insert(ctx, Point{Timestamp: 1, Value: 2}))

	infos, err := store.ListSeries(ctx)
	require.NoError(t, err)
	require.Len(t, infos, 2)
	require.Equal(t, "cpu", infos[0].Name)
	require.Equal(t, "mem", infos[1].Name)
	require.Equal(t, "a", infos[0].Labels["host"])
	require.Equal(t, 10, infos[0].Retention.MaxPoints)
	require.Equal(t, int64(1), infos[0].Count)
	require.Nil(t, infos[1].Labels)

	// A second Store over the same handle has an empty in-memory series set,
	// exactly like a process restart, but ListSeries reads them from disk.
	reopened, err := openStore(ctx, db, root, Config{})
	require.NoError(t, err)

	names, err := reopened.ListSeriesNames(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"cpu", "mem"}, names)

	// Deleting a series removes it from the enumeration.
	require.NoError(t, reopened.DeleteSeries(ctx, "cpu"))
	names, err = reopened.ListSeriesNames(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"mem"}, names)
}

// TestListSeriesEncryptedNames verifies enumeration recovers the logical names
// when series names are encrypted.
func TestListSeriesEncryptedNames(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")
	store, _ := openAEADTestStore(t, root, Config{Secret: testKey(33), EncryptSeriesNames: true, EncryptValues: true})

	require.NoError(t, store.Series("cpu").Insert(ctx, Point{Timestamp: 1, Value: 1}))
	require.NoError(t, store.Series("mem").Insert(ctx, Point{Timestamp: 1, Value: 2}))

	names, err := store.ListSeriesNames(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"cpu", "mem"}, names)
}
