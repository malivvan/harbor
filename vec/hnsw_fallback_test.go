package vec

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/harbor/metrics"
)

// fallbackCounter records how many times the exact-scan fallback fired.
type fallbackCounter struct {
	fallbacks int64
}

func (c *fallbackCounter) Counter(name string, delta int64, _ metrics.Labels) {
	if name == metrics.MetricVecFallbacks {
		c.fallbacks += delta
	}
}
func (c *fallbackCounter) Histogram(string, float64, metrics.Labels) {}
func (c *fallbackCounter) Gauge(string, float64, metrics.Labels)     {}
func (c *fallbackCounter) StartSpan(ctx context.Context, _ string, _ metrics.Labels) (context.Context, func(error)) {
	return ctx, func(error) {}
}

func scoredIDs(points []ScoredPoint) []string {
	out := make([]string, len(points))
	for i, p := range points {
		out[i] = p.ID
	}
	return out
}

// TestHNSWFilteredSearchFallback verifies that when a filtered graph search
// cannot fill the requested result set, the exact-scan fallback returns the
// same points as the flat index, and that the fallback is reported.
func TestHNSWFilteredSearchFallback(t *testing.T) {
	points := make([]PointStruct, 100)
	for i := range points {
		role := "common"
		switch {
		case i%25 == 0:
			role = "rare" // 4 points
		case i == 7:
			role = "unique" // 1 point
		}
		points[i] = PointStruct{
			ID:      fmt.Sprintf("p%03d", i),
			Vector:  []float32{float32(i % 10), float32(i / 10)},
			Payload: Payload{"role": role, "i": i},
		}
	}

	flat := NewFlatIndex(Euclid)
	hnswIdx := NewHNSWIndex(Euclid)
	require.NoError(t, flat.Upsert(points))
	require.NoError(t, hnswIdx.Upsert(points))

	obs := &fallbackCounter{}
	hnswIdx.obs = obs

	query := []float32{5, 5}
	cases := []struct {
		role  string
		topK  int
		exact bool
	}{
		{"unique", 5, true}, // 1 match < topK -> exact fallback
		{"rare", 5, true},   // 4 matches < topK -> exact fallback
		{"absent", 5, true}, // no matches -> exact fallback (empty)
		{"common", 5, false},
	}
	for _, tc := range cases {
		filter := &Filter{Must: []Condition{{Key: "role", Match: MatchValue{Value: tc.role}}}}
		want, err := flat.Search(query, filter, tc.topK)
		require.NoError(t, err)
		got, err := hnswIdx.Search(query, filter, tc.topK)
		require.NoError(t, err)
		if tc.exact {
			require.Equal(t, scoredIDs(want), scoredIDs(got), "role=%s", tc.role)
		} else {
			require.Len(t, got, tc.topK)
		}
	}

	require.Positive(t, obs.fallbacks, "sparse filters should trigger the exact-scan fallback")
}
