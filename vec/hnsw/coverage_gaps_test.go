package hnsw

import (
	"bytes"
	"errors"
	"os"
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

func TestCandidateSetCleanMin(t *testing.T) {
	var set candidateSet[int]
	n1 := &layerNode[int]{Node: Node[int]{Key: 1}}
	n2 := &layerNode[int]{Node: Node[int]{Key: 2}}
	set.Push(n1, 1)
	set.Push(n2, 2)

	// PopMin marks the root dead in both heaps; the next Min must clean it.
	require.Equal(t, n1, set.PopMin().node)
	require.Equal(t, 1, set.Len())
	require.Equal(t, n2, set.Min().node)
	require.Equal(t, n2, set.PopMin().node)
	require.Zero(t, set.Len())
}

func TestLayerSize(t *testing.T) {
	require.Zero(t, (&layer[string]{}).size())
	l := &layer[string]{nodes: map[string]*layerNode[string]{
		"a": {},
		"b": {},
	}}
	require.Equal(t, 2, l.size())
}

func TestSearchWithDistancesEdges(t *testing.T) {
	var empty Graph[string]
	require.Nil(t, empty.SearchWithDistances([]float32{1}, 1))
	require.Nil(t, empty.SearchWithDistances([]float32{1}, 0))

	g := graphWithNodes(5)
	// k larger than the graph is clamped.
	got := g.SearchWithDistances([]float32{1, 1, 1}, 100)
	require.Len(t, got, 5)
	for _, node := range got {
		require.Equal(t, g.Distance([]float32{1, 1, 1}, node.Node.Value), node.Distance)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

func TestExportImportAndSaveErrors(t *testing.T) {
	g := graphWithNodes(3)

	var buf bytes.Buffer
	require.NoError(t, g.Export(&buf))
	require.NotZero(t, buf.Len())

	var imported Graph[string]
	require.NoError(t, imported.Import(bytes.NewReader(buf.Bytes())))
	require.Equal(t, g.Len(), imported.Len())

	// A truncated stream is rejected.
	require.Error(t, imported.Import(bytes.NewReader(buf.Bytes()[:4])))
	require.Error(t, imported.Import(bytes.NewReader(nil)))

	// Export to a failing writer.
	require.Error(t, g.Export(failingWriter{}))

	// Saving into a missing directory fails.
	saved := &SavedGraph[string]{Graph: g, Path: filepath.Join(t.TempDir(), "missing", "g.bin")}
	require.Error(t, saved.Save())

	// Loading a corrupt file fails.
	corrupt := filepath.Join(t.TempDir(), "corrupt.bin")
	require.NoError(t, os.WriteFile(corrupt, []byte("not a graph"), 0600))
	_, err := LoadSavedGraph[string](corrupt)
	require.Error(t, err)
}

func TestLayerNilSizeAndDrainedClean(t *testing.T) {
	var nilLayer *layer[string]
	require.Zero(t, nilLayer.size())

	var set candidateSet[int]
	set.Push(&layerNode[int]{Node: Node[int]{Key: 1}}, 1)
	require.Equal(t, 1, set.PopMin().node.Key)
	set.cleanMin()
	require.Zero(t, len(set.min))
	set.cleanMax()
	require.Zero(t, len(set.max))
}
