package vector

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetPoint(t *testing.T) {
	store := openTestStorage(t, filepath.Join(t.TempDir(), "getpoint.db"))

	col, err := store.NewCollection("docs", 2, Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]PointStruct{
		{ID: "a", Vector: []float32{1, 0}, Payload: Payload{"role": "admin"}},
		{ID: "b", Vector: []float32{0, 1}},
		{ID: "c", Vector: []float32{1, 1}},
	}))

	// The in-memory index sees the upsert immediately, before any flush.
	got, err := col.Get("a")
	require.NoError(t, err)
	require.Equal(t, "a", got.ID)
	require.Equal(t, []float32{1, 0}, got.Vector)
	require.Equal(t, "admin", got.Payload["role"])

	_, err = col.Get("missing")
	require.ErrorIs(t, err, ErrPointNotFound)

	// Durable retrieval sees only flushed points.
	_, err = store.GetPoint("docs", "b")
	require.ErrorIs(t, err, ErrPointNotFound)

	require.NoError(t, col.Flush())

	got, err = store.GetPoint("docs", "b")
	require.NoError(t, err)
	require.Equal(t, "b", got.ID)
	require.Equal(t, []float32{0, 1}, got.Vector)

	_, err = store.GetPoint("docs", "missing")
	require.ErrorIs(t, err, ErrPointNotFound)

	_, err = store.GetPoint("nope", "b")
	require.ErrorIs(t, err, ErrCollectionNotFound)
}

func TestGetPoints(t *testing.T) {
	store := openTestStorage(t, filepath.Join(t.TempDir(), "getpoints.db"))

	col, err := store.NewCollection("docs", 2, Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]PointStruct{
		{ID: "a", Vector: []float32{1, 0}},
		{ID: "b", Vector: []float32{0, 1}},
	}))
	require.NoError(t, col.Flush())

	found, missing, err := store.GetPoints("docs", []string{"a", "x", "b", "x"})
	require.NoError(t, err)
	require.Len(t, found, 2)
	require.Contains(t, found, "a")
	require.Contains(t, found, "b")
	require.Equal(t, []string{"x"}, missing)

	_, _, err = store.GetPoints("nope", []string{"a"})
	require.ErrorIs(t, err, ErrCollectionNotFound)
}
