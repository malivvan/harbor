package vector_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/tempest/internal/evals"
	"github.com/malivvan/tempest/store/vector"
)

// TestHNSWRecallFloor is the retrieval-quality gate from GAP-33. The corpus is
// deterministic, so the floor is a regression contract: a change to the HNSW
// parameters, the distance kernel or the seed handling that drops recall below
// it fails here instead of shipping silently.
func TestHNSWRecallFloor(t *testing.T) {
	const (
		n     = 300
		dim   = 8
		topK  = 10
		floor = 0.80
	)
	vectors, queries := evals.SyntheticCorpus(n, dim, 42)
	require.NotEmpty(t, vectors)
	require.NotEmpty(t, queries)

	points := make([]vector.PointStruct, n)
	for i, v := range vectors {
		points[i] = vector.PointStruct{ID: fmt.Sprintf("p%03d", i), Vector: v}
	}

	flat := vector.NewFlatIndex(vector.Euclid)
	hnsw := vector.NewHNSWIndex(vector.Euclid)
	require.NoError(t, flat.Upsert(points))
	require.NoError(t, hnsw.Upsert(points))

	total := 0.0
	for _, q := range queries {
		exact, err := flat.Search(q, nil, topK)
		require.NoError(t, err)
		approx, err := hnsw.Search(q, nil, topK)
		require.NoError(t, err)
		total += evals.RecallAtK(ids(exact), ids(approx), topK)
	}
	recall := total / float64(len(queries))
	if recall < floor {
		t.Fatalf("HNSW recall %.3f is below the %.2f floor", recall, floor)
	}
}

func ids(points []vector.ScoredPoint) []string {
	out := make([]string, len(points))
	for i, point := range points {
		out[i] = point.ID
	}
	return out
}
