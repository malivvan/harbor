package hnsw

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func graphWithNodes(n int) *Graph[string] {
	g := NewGraph[string]()
	nodes := make([]Node[string], n)
	for i := range nodes {
		vec := []float32{float32(i), float32(i % 3), 1}
		nodes[i] = MakeNode(string(rune('a'+i%26))+string(rune('0'+i/26)), vec)
	}
	g.Add(nodes...)
	return g
}

func TestSavedGraphRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "graph.bin")

	saved, err := LoadSavedGraph[string](path)
	require.NoError(t, err)
	require.NotNil(t, saved.Graph)
	require.Zero(t, saved.Graph.Len())

	g := saved.Graph
	g.Add(
		MakeNode("a", []float32{1, 0, 0}),
		MakeNode("b", []float32{0, 1, 0}),
		MakeNode("c", []float32{1, 1, 0}),
	)
	require.NoError(t, saved.Save())
	require.FileExists(t, path)

	loaded, err := LoadSavedGraph[string](path)
	require.NoError(t, err)
	require.Equal(t, 3, loaded.Graph.Len())

	vec, ok := loaded.Graph.Lookup("a")
	require.True(t, ok)
	require.Equal(t, []float32{1, 0, 0}, vec)

	got := loaded.Graph.Search([]float32{1, 0, 0}, 2)
	require.NotEmpty(t, got)

	// Saving again replaces the file atomically.
	loaded.Graph.Add(MakeNode("d", []float32{0, 0, 1}))
	require.NoError(t, loaded.Save())
	again, err := LoadSavedGraph[string](path)
	require.NoError(t, err)
	require.Equal(t, 4, again.Graph.Len())
}

func TestGraphLookupDeleteAndReplace(t *testing.T) {
	var empty Graph[string]
	require.Zero(t, empty.Len())
	require.False(t, empty.Delete("x"))
	_, ok := empty.Lookup("x")
	require.False(t, ok)
	require.Zero(t, empty.Dims())
	require.Nil(t, empty.Search([]float32{1}, 1))

	g := graphWithNodes(6)
	require.Equal(t, 6, g.Len())
	require.Equal(t, 3, g.Dims())

	_, ok = g.Lookup("missing")
	require.False(t, ok)

	// Replacing an existing key keeps the graph size and updates the vector.
	key := g.layers[0].entry().Key
	g.Add(MakeNode(key, []float32{9, 9, 9}))
	require.Equal(t, 6, g.Len())
	vec, ok := g.Lookup(key)
	require.True(t, ok)
	require.Equal(t, []float32{9, 9, 9}, vec)

	require.True(t, g.Delete(key))
	require.False(t, g.Delete(key))
	_, ok = g.Lookup(key)
	require.False(t, ok)
	require.Equal(t, 5, g.Len())
}

func TestAnalyzer(t *testing.T) {
	var nilAnalyzer *Analyzer[string]
	require.Zero(t, nilAnalyzer.Height())
	require.Zero(t, (&Analyzer[string]{}).Height())

	g := graphWithNodes(20)
	a := &Analyzer[string]{Graph: g}
	require.NotZero(t, a.Height())
	connectivity := a.Connectivity()
	require.NotEmpty(t, connectivity)
	for _, c := range connectivity {
		require.GreaterOrEqual(t, c, float64(0))
	}
	topography := a.Topography()
	require.NotEmpty(t, topography)
	require.Equal(t, 20, topography[0])

	empty := &Analyzer[string]{Graph: NewGraph[string]()}
	require.Empty(t, empty.Connectivity())
	require.Empty(t, empty.Topography())
}

func TestMapsKeysAndCandidateLess(t *testing.T) {
	keys := mapsKeys(map[string]int{"a": 1, "b": 2})
	require.ElementsMatch(t, []string{"a", "b"}, keys)

	low := searchCandidate[string]{dist: 1}
	high := searchCandidate[string]{dist: 2}
	require.True(t, low.Less(high))
	require.False(t, high.Less(low))
}
