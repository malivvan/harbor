package evals

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecallAtK(t *testing.T) {
	exact := []string{"a", "b", "c", "d"}
	approx := []string{"a", "x", "b", "y"}
	require.InDelta(t, 0.5, RecallAtK(exact, approx, 4), 1e-9)
	require.InDelta(t, 1.0, RecallAtK([]string{"a", "b"}, []string{"a", "b"}, 2), 1e-9)
	require.Zero(t, RecallAtK(nil, approx, 4))
	require.Zero(t, RecallAtK(exact, approx, 0))
	// Duplicates in the approximate list do not inflate the denominator.
	require.InDelta(t, 1.0, RecallAtK([]string{"a", "b"}, []string{"a", "a", "b"}, 2), 1e-9)
}

func TestNDCG(t *testing.T) {
	grades := map[string]int{"a": 3, "b": 2, "c": 1}
	require.InDelta(t, 1.0, NDCG(grades, []string{"a", "b", "c"}, 3), 1e-9)
	require.Greater(t, NDCG(grades, []string{"a", "b", "c"}, 3), NDCG(grades, []string{"c", "b", "a"}, 3))
	require.Zero(t, NDCG(nil, []string{"a"}, 1))
	require.Zero(t, NDCG(map[string]int{"a": 0}, []string{"a"}, 1))
}

func TestMRR(t *testing.T) {
	require.InDelta(t, 0.5, MRR([]string{"x", "a"}, map[string]bool{"a": true}), 1e-9)
	require.InDelta(t, 0.25, MRR([]string{"x", "y", "z", "a"}, map[string]bool{"a": true}), 1e-9)
	require.Zero(t, MRR([]string{"x"}, map[string]bool{"a": true}))
}
