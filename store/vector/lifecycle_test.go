package vector

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	bbolterrors "go.etcd.io/bbolt/errors"
)

func openSharedStore(t *testing.T) (*Store, *bbolt.DB) {
	t.Helper()
	bdb, err := bbolt.Open(t.TempDir()+"/shared.db", 0600, &bbolt.Options{Timeout: 1e9})
	require.NoError(t, err)
	store, err := Open(context.Background(), bdb, Config{SharedDB: true})
	require.NoError(t, err)
	return store, bdb
}

// TestSharedDBLeavesHandleOpen pins the embedding lifecycle: Close flushes
// buffered points and releases the wrapper, but never closes the caller's
// handle.
func TestSharedDBLeavesHandleOpen(t *testing.T) {
	store, bdb := openSharedStore(t)
	defer func() { require.NoError(t, bdb.Close()) }()

	col, err := store.NewCollection("docs", 2, Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]PointStruct{{ID: "a", Vector: []float32{1, 0}}}))
	require.Equal(t, 1, col.Pending())

	require.False(t, store.Closed())
	require.NoError(t, store.Close())
	require.True(t, store.Closed())
	require.ErrorIs(t, store.Flush(), ErrClosed)
	require.NoError(t, store.Close(), "Close is idempotent")

	// The handle is still open and holds the flushed point.
	require.NoError(t, bdb.Update(func(tx *bbolt.Tx) error { return nil }))

	again, err := Open(context.Background(), bdb, Config{SharedDB: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, again.Close()) }()
	points, err := again.LoadCollection("docs")
	require.NoError(t, err)
	require.Contains(t, points, "a")
}

// TestStandaloneStoreClosesHandle pins the default: without SharedDB, Close
// owns and closes the handle.
func TestStandaloneStoreClosesHandle(t *testing.T) {
	bdb, err := bbolt.Open(t.TempDir()+"/owned.db", 0600, &bbolt.Options{Timeout: 1e9})
	require.NoError(t, err)
	store, err := Open(context.Background(), bdb, Config{})
	require.NoError(t, err)

	require.False(t, store.Closed())
	require.NoError(t, store.Close())
	require.True(t, store.Closed())
	require.ErrorIs(t, store.Flush(), ErrClosed)
	require.ErrorIs(t, bdb.Update(func(tx *bbolt.Tx) error { return nil }), bbolterrors.ErrDatabaseNotOpen)
}

// TestCollectionCloseIsTerminal checks the explicit collection state: Close
// flushes, retires the handle, and later operations fail with ErrClosed.
func TestCollectionCloseIsTerminal(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]PointStruct{{ID: "a", Vector: []float32{1, 0}}}))
	require.False(t, col.Closed())
	require.Equal(t, 1, col.Pending())

	require.NoError(t, col.Close())
	require.True(t, col.Closed())
	require.Equal(t, 0, col.Pending())

	require.ErrorIs(t, col.Upsert([]PointStruct{{ID: "b", Vector: []float32{0, 1}}}), ErrClosed)
	require.ErrorIs(t, col.Flush(), ErrClosed)
	_, err = col.Search([]float32{1, 0}, nil, 1)
	require.ErrorIs(t, err, ErrClosed)
	_, err = col.Get("a")
	require.ErrorIs(t, err, ErrClosed)
	_, err = col.GetPointsByFilter(nil)
	require.ErrorIs(t, err, ErrClosed)
	_, err = col.ListPoints(nil, ListOptions{})
	require.ErrorIs(t, err, ErrClosed)
	require.ErrorIs(t, col.ForEachPoint(nil, func(PointStruct) error { return nil }), ErrClosed)
	_, err = col.Delete([]string{"a"}, nil)
	require.ErrorIs(t, err, ErrClosed)

	// Close is idempotent and the flushed point is durable.
	require.NoError(t, col.Close())
	points, err := store.LoadCollection("docs")
	require.NoError(t, err)
	require.Contains(t, points, "a")

	// Store.Flush/Close skip a retired collection instead of failing on it.
	require.NoError(t, store.Flush())
	require.NoError(t, store.Close())
}

func TestCollectionPendingTracksBuffer(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollectionWithOptions(CollectionOptions{
		Name: "docs", VectorLen: 2, Metric: Cosine, FlushThreshold: 10,
	})
	require.NoError(t, err)

	require.NoError(t, col.Upsert([]PointStruct{
		{ID: "a", Vector: []float32{1, 0}},
		{ID: "b", Vector: []float32{0, 1}},
	}))
	require.Equal(t, 2, col.Pending())

	require.NoError(t, col.Flush())
	require.Equal(t, 0, col.Pending())

	require.NoError(t, col.Upsert([]PointStruct{{ID: "c", Vector: []float32{1, 1}}}))
	require.Equal(t, 1, col.Pending())

	require.NoError(t, col.Close())
	require.Equal(t, 0, col.Pending())
	require.True(t, col.Closed())
}

// TestNilLifecycleHandles checks that nil handles report a closed state instead
// of panicking.
func TestNilLifecycleHandles(t *testing.T) {
	var store *Store
	require.True(t, store.Closed())
	require.ErrorIs(t, store.Flush(), ErrClosed)
	require.NoError(t, store.Close())

	var col *Collection
	require.True(t, col.Closed())
	require.Equal(t, 0, col.Pending())
	require.NoError(t, col.Close())
}
