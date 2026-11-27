package vector

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func openVectorExpiryStore(t *testing.T) (*Store, *Collection) {
	t.Helper()
	bdb, err := bbolt.Open(filepath.Join(t.TempDir(), "vector-expiry.db"), 0o600, &bbolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	store, err := Open(context.Background(), bdb, Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	col, err := store.NewCollection("docs", 2, Cosine, false)
	require.NoError(t, err)
	return store, col
}

func seedExpiryPoints(t *testing.T, col *Collection) {
	t.Helper()
	require.NoError(t, col.Upsert([]PointStruct{
		{ID: "a", Vector: []float32{1, 0}, Payload: Payload{"topic": "x"}},
		{ID: "b", Vector: []float32{0, 1}, Payload: Payload{"topic": "y"}},
	}))
}

func TestVectorExpiryHidesFromReadsAndSweeps(t *testing.T) {
	store, col := openVectorExpiryStore(t)
	seedExpiryPoints(t, col)
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	store.SetNow(func() time.Time { return now })

	require.NoError(t, col.SetExpiry("a", now.Add(-time.Minute)))
	at, ok, err := col.Expiry("a")
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, at.Equal(now.Add(-time.Minute)))

	_, err = col.Get("a")
	require.ErrorIs(t, err, ErrPointNotFound)

	hits, err := col.Search([]float32{1, 0}, nil, 10)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, "b", hits[0].ID)

	points, err := col.GetPointsByFilter(nil)
	require.NoError(t, err)
	require.Len(t, points, 1)
	require.Equal(t, "b", points[0].ID)

	listed, err := col.ListPoints(nil, ListOptions{})
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, "b", listed[0].ID)

	ranged, err := col.RangeSearch([]float32{1, 0}, 2, nil)
	require.NoError(t, err)
	require.Len(t, ranged, 1)
	require.Equal(t, "b", ranged[0].ID)

	// ForEachPoint is the maintenance view: it sees expired points so a sweep
	// can find them.
	var all []string
	require.NoError(t, col.ForEachPoint(nil, func(p PointStruct) error {
		all = append(all, p.ID)
		return nil
	}))
	require.ElementsMatch(t, []string{"a", "b"}, all)

	removed, err := col.SweepExpired(now, 0)
	require.NoError(t, err)
	require.Equal(t, 1, removed)
	require.Equal(t, 1, col.Count())
}

func TestVectorExpiryZeroClears(t *testing.T) {
	store, col := openVectorExpiryStore(t)
	seedExpiryPoints(t, col)
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	store.SetNow(func() time.Time { return now })

	require.NoError(t, col.SetExpiry("a", now.Add(-time.Minute)))
	require.ErrorIs(t, func() error { _, err := col.Get("a"); return err }(), ErrPointNotFound)

	require.NoError(t, col.SetExpiry("a", time.Time{}))
	_, ok, err := col.Expiry("a")
	require.NoError(t, err)
	require.False(t, ok)
	point, err := col.Get("a")
	require.NoError(t, err)
	require.Equal(t, "a", point.ID)
}

func TestVectorExpiryPersistsAndUpgradesFormat(t *testing.T) {
	store, col := openVectorExpiryStore(t)
	seedExpiryPoints(t, col)
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	store.SetNow(func() time.Time { return now })

	require.NoError(t, col.SetExpiry("a", now.Add(time.Hour)))
	require.NoError(t, col.Flush())

	meta, err := store.LoadCollectionMeta("docs")
	require.NoError(t, err)
	require.Equal(t, CollectionFormatVersionExpiry, meta.FormatVersion)

	// Reopening the same database sees the expiry and still filters once the
	// clock passes it.
	reopened, err := Open(context.Background(), store.db, Config{})
	require.NoError(t, err)
	reopened.SetNow(func() time.Time { return now.Add(2 * time.Hour) })
	col2, err := reopened.Collection("docs")
	require.NoError(t, err)

	_, err = col2.Get("a")
	require.ErrorIs(t, err, ErrPointNotFound)
	hits, err := col2.Search([]float32{1, 0}, nil, 10)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, "b", hits[0].ID)
}

func TestVectorExpiryMissingPoint(t *testing.T) {
	_, col := openVectorExpiryStore(t)
	require.ErrorIs(t, col.SetExpiry("missing", time.Now()), ErrPointNotFound)
	_, _, err := col.Expiry("missing")
	require.ErrorIs(t, err, ErrPointNotFound)
}
