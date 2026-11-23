package vector

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func flatTestPoints(n, dim int) []PointStruct {
	points := make([]PointStruct, n)
	for i := 0; i < n; i++ {
		vector := make([]float32, dim)
		for d := range vector {
			vector[d] = float32((i*31+d*17)%101) / 101
		}
		points[i] = PointStruct{
			ID:      fmt.Sprintf("p%d", i),
			Vector:  vector,
			Version: uint64(i),
			Payload: map[string]any{
				"bucket": i % 5,
				"nested": map[string]any{"i": i},
			},
		}
	}
	return points
}

// TestFlatSearchTopKMatchesBruteForce guards the bounded-heap top-K selection
// and result ordering against a full sort for every metric.
func TestFlatSearchTopKMatchesBruteForce(t *testing.T) {
	for _, metric := range []Distance{Euclid, Cosine, Dot} {
		t.Run(string(metric), func(t *testing.T) {
			idx := NewFlatIndex(metric)
			points := flatTestPoints(500, 16)
			require.NoError(t, idx.Upsert(points))

			query := make([]float32, 16)
			for i := range query {
				query[i] = float32(i%7) / 7
			}

			for _, topK := range []int{1, 3, 10, 500, 900} {
				got, err := idx.Search(query, nil, topK)
				require.NoError(t, err)

				want := make([]ScoredPoint, 0, len(points))
				for _, p := range points {
					want = append(want, ScoredPoint{
						ID:      p.ID,
						Version: p.Version,
						Score:   CalculateDistance(metric, query, p.Vector),
					})
				}
				sort.Slice(want, func(i, j int) bool {
					if metric == Euclid {
						return want[i].Score < want[j].Score
					}
					return want[i].Score > want[j].Score
				})
				if topK < len(want) {
					want = want[:topK]
				}

				require.Len(t, got, len(want))

				// Ties are ordered arbitrarily, so compare the score at each
				// rank and verify every returned ID really has that score.
				scores := make(map[string]float32, len(points))
				for _, p := range points {
					scores[p.ID] = CalculateDistance(metric, query, p.Vector)
				}
				seen := make(map[string]struct{}, len(got))
				for i := range want {
					require.InDelta(t, want[i].Score, got[i].Score, 1e-6, "rank %d", i)
					require.InDelta(t, scores[got[i].ID], got[i].Score, 1e-6)
					_, dup := seen[got[i].ID]
					require.False(t, dup, "duplicate id %s", got[i].ID)
					seen[got[i].ID] = struct{}{}
				}
			}
		})
	}
}

// TestFlatSearchPayloadsAreIndependent verifies that returned payloads are
// deep copies, even though cloning now happens only for top-K survivors.
func TestFlatSearchPayloadsAreIndependent(t *testing.T) {
	idx := NewFlatIndex(Euclid)
	points := flatTestPoints(20, 8)
	require.NoError(t, idx.Upsert(points))

	query := make([]float32, 8)
	got, err := idx.Search(query, nil, 3)
	require.NoError(t, err)
	require.Len(t, got, 3)

	got[0].Payload["bucket"] = "mutated"
	nested := got[0].Payload["nested"].(map[string]any)
	nested["i"] = -1

	fresh, err := idx.Search(query, nil, 3)
	require.NoError(t, err)
	require.NotEqual(t, "mutated", fresh[0].Payload["bucket"])
	require.NotEqual(t, -1, fresh[0].Payload["nested"].(map[string]any)["i"])
}

// TestStoredPayloadUsesOneJSONDocument verifies that complex payload values
// are encoded into a single reserved JSON document and round-trip correctly.
func TestStoredPayloadUsesOneJSONDocument(t *testing.T) {
	point := &PointStruct{
		ID:      "p1",
		Vector:  []float32{1, 2, 3},
		Version: 7,
		Payload: Payload{
			"scalar": 42,
			"name":   "x",
			"nested": map[string]any{"n": 3},
			"list":   []any{float64(1), "two"},
		},
	}

	stored, err := toStoredPoint(point)
	require.NoError(t, err)
	require.Len(t, stored.Payload, 3)

	jsonDocs := 0
	for k, v := range stored.Payload {
		if v != nil && v.Kind == KindJSON {
			jsonDocs++
			require.Equal(t, payloadJSONKey, k)
		}
	}
	require.Equal(t, 1, jsonDocs, "complex values must share one JSON document")

	got := fromStoredPoint(stored)
	require.Equal(t, point.ID, got.ID)
	require.Equal(t, point.Vector, got.Vector)
	require.Equal(t, point.Version, got.Version)
	require.EqualValues(t, 42, got.Payload["scalar"])
	require.Equal(t, "x", got.Payload["name"])
	require.EqualValues(t, 3, got.Payload["nested"].(map[string]any)["n"])
	require.Equal(t, "two", got.Payload["list"].([]any)[1])
}
