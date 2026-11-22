package hnsw

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAnalyzerHeightCountsNonEmptyLayers covers N3: trailing empty layers left
// by Delete must not inflate Height.
func TestAnalyzerHeightCountsNonEmptyLayers(t *testing.T) {
	g := NewGraph[string]()
	g.Add(
		MakeNode("a", Vector{1, 0}),
		MakeNode("b", Vector{0, 1}),
		MakeNode("c", Vector{1, 1}),
	)

	analyzer := &Analyzer[string]{Graph: g}
	require.Greater(t, analyzer.Height(), 0)

	heightBefore := analyzer.Height()
	require.LessOrEqual(t, heightBefore, len(g.layers))

	for _, key := range []string{"a", "b", "c"} {
		require.True(t, g.Delete(key))
	}

	require.Equal(t, 0, analyzer.Height(), "all layers are empty after deleting every node")
	for _, count := range analyzer.Topography() {
		require.Zero(t, count)
	}
	require.Empty(t, analyzer.Connectivity())
}

func TestAnalyzerNilGraph(t *testing.T) {
	var analyzer *Analyzer[string]
	require.Zero(t, analyzer.Height())
}

// TestGraphSearchNilDistance covers N4: a hand-built graph without a Distance
// function must not panic.
func TestGraphSearchNilDistance(t *testing.T) {
	g := NewGraph[string]()
	g.Add(MakeNode("a", Vector{1, 0}), MakeNode("b", Vector{0, 1}))
	g.Distance = nil

	require.NotPanics(t, func() {
		require.Nil(t, g.Search(Vector{1, 0}, 1))
	})
}

// TestGraphSearchNonPositiveEfSearch covers N4: EfSearch <= 0 must not collapse
// the traversal to the direct neighbours.
func TestGraphSearchNonPositiveEfSearch(t *testing.T) {
	g := NewGraph[string]()
	nodes := make([]Node[string], 0, 50)
	for i := 0; i < 50; i++ {
		nodes = append(nodes, MakeNode(string(rune('A'+i)), Vector{float32(i), 1}))
	}
	g.Add(nodes...)

	for _, ef := range []int{0, -1} {
		g.EfSearch = ef
		results := g.Search(Vector{0, 1}, 5)
		require.Len(t, results, 5, "EfSearch=%d must still return the requested k", ef)
	}
}

// TestZeroValueGraphSearch covers N5: a zero-value Graph must be safe to query.
func TestZeroValueGraphSearch(t *testing.T) {
	var g Graph[string]
	require.NotPanics(t, func() {
		require.Nil(t, g.Search(Vector{1, 0}, 1))
	})
}
