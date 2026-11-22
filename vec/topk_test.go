package vec

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// topK is caller-controlled and used to size an allocation. A huge value must
// be clamped to the number of stored points instead of panicking or attempting
// a multi-gigabyte allocation.
func TestFlatIndexSearchClampsTopK(t *testing.T) {
	idx := NewFlatIndex(Euclid)

	// Empty index: any topK is safe and yields no results.
	results, err := idx.Search([]float32{0, 0, 0}, nil, math.MaxInt)
	require.NoError(t, err)
	require.Empty(t, results)

	require.NoError(t, idx.Upsert([]PointStruct{
		{ID: "a", Vector: []float32{0, 0, 0}},
		{ID: "b", Vector: []float32{1, 0, 0}},
		{ID: "c", Vector: []float32{2, 0, 0}},
	}))

	for _, topK := range []int{math.MaxInt, 1 << 30, 1_000_000} {
		results, err = idx.Search([]float32{0, 0, 0}, nil, topK)
		require.NoError(t, err, "topK=%d", topK)
		require.Len(t, results, 3, "topK=%d", topK)
	}

	// A normal topK still limits the result set.
	results, err = idx.Search([]float32{0, 0, 0}, nil, 2)
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, "a", results[0].ID)

	// Non-positive values remain rejected.
	_, err = idx.Search([]float32{0, 0, 0}, nil, 0)
	require.Error(t, err)
	_, err = idx.Search([]float32{0, 0, 0}, nil, -5)
	require.Error(t, err)
}
