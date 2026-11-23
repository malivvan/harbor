package vector

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewCollectionWithOptions(t *testing.T) {
	store := openTestStorage(t, filepath.Join(t.TempDir(), "options.db"))

	col, err := store.NewCollectionWithOptions(CollectionOptions{
		Name:           "docs",
		VectorLen:      3,
		Metric:         Euclid,
		UseHNSW:        true,
		FlushThreshold: 2,
	})
	require.NoError(t, err)
	require.Equal(t, 3, col.VectorLen)
	require.Equal(t, Euclid, col.Metric)
	_, ok := col.index.(*HNSWIndex)
	require.True(t, ok, "UseHNSW should select the HNSW index")

	// FlushThreshold is honored: the first point stays buffered, the second
	// trips the threshold and flushes both to durable storage.
	require.NoError(t, col.Upsert([]PointStruct{{ID: "a", Vector: []float32{1, 0, 0}}}))
	_, err = store.GetPoint("docs", "a")
	require.ErrorIs(t, err, ErrPointNotFound)

	require.NoError(t, col.Upsert([]PointStruct{{ID: "b", Vector: []float32{0, 1, 0}}}))
	_, err = store.GetPoint("docs", "a")
	require.NoError(t, err)
	_, err = store.GetPoint("docs", "b")
	require.NoError(t, err)
}

func TestNewCollectionWithOptionsValidation(t *testing.T) {
	store := openTestStorage(t, filepath.Join(t.TempDir(), "options-bad.db"))

	_, err := store.NewCollectionWithOptions(CollectionOptions{Metric: Cosine})
	require.ErrorIs(t, err, ErrInvalidConfig)

	_, err = store.NewCollectionWithOptions(CollectionOptions{Name: "x", VectorLen: 0, Metric: Cosine})
	require.ErrorIs(t, err, ErrInvalidConfig)

	_, err = store.NewCollectionWithOptions(CollectionOptions{Name: "x", VectorLen: 2, Metric: Distance("bogus")})
	require.ErrorIs(t, err, ErrInvalidConfig)
}
