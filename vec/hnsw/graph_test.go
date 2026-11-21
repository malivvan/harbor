package hnsw

import (
	"math/rand"
	"slices"
	"testing"
)

// zeroSource yields 0 for every draw, so randomLevel always picks the maximum
// level. It makes level generation deterministic in tests.
type zeroSource struct{}

func (zeroSource) Int63() int64 { return 0 }
func (zeroSource) Seed(int64)   {}

// halfSource yields 0.5 for every draw, so a graph with Ml = 0.25 always picks
// level 0 (0.5 > 0.25).
type halfSource struct{}

func (halfSource) Int63() int64 { return 1 << 62 }
func (halfSource) Seed(int64)   {}

func newTestGraph() *Graph[int] {
	g := NewGraph[int]()
	g.M = 4
	g.Ml = 0.25
	g.EfSearch = 50
	g.EfConstruction = 100
	g.Distance = EuclideanDistance
	g.Rng = rand.New(zeroSource{})
	return g
}

// TestGraphSearchResultsAreSorted is a regression test for search returning the
// raw min-heap array: only its first element used to be the nearest one, so the
// top-K was neither complete nor ordered.
func TestGraphSearchResultsAreSorted(t *testing.T) {
	g := newTestGraph()
	g.Rng = rand.New(rand.NewSource(42))

	const n = 200
	for i := 0; i < n; i++ {
		g.Add(MakeNode(i, Vector{float32(i), float32(i % 5)}))
	}

	query := Vector{10.4, 1}
	const k = 10
	got := g.Search(query, k)
	if len(got) != k {
		t.Fatalf("Search() returned %d nodes, want %d", len(got), k)
	}

	for i := 1; i < len(got); i++ {
		prev := EuclideanDistance(got[i-1].Value, query)
		cur := EuclideanDistance(got[i].Value, query)
		if prev > cur {
			t.Fatalf("results are not sorted by ascending distance: %v > %v at index %d", prev, cur, i)
		}
	}

	// The retained set must be the true k nearest neighbors, which can be
	// computed by brute force on this well-separated data set.
	type scored struct {
		key  int
		dist float32
	}
	all := make([]scored, 0, n)
	for i := 0; i < n; i++ {
		all = append(all, scored{i, EuclideanDistance(Vector{float32(i), float32(i % 5)}, query)})
	}
	slices.SortFunc(all, func(a, b scored) int {
		switch {
		case a.dist < b.dist:
			return -1
		case a.dist > b.dist:
			return 1
		default:
			return 0
		}
	})

	for i, node := range got {
		if node.Key != all[i].key {
			t.Fatalf("Search() result #%d = node %d, want node %d", i, node.Key, all[i].key)
		}
	}
}

// TestGraphSearchKIsOrderedForCosine checks the ordering for the default
// cosine metric as well.
func TestGraphSearchKIsOrderedForCosine(t *testing.T) {
	g := newTestGraph()
	g.Distance = CosineDistance
	g.Rng = rand.New(rand.NewSource(7))

	for i := 0; i < 50; i++ {
		g.Add(MakeNode(i, Vector{float32(i + 1), float32(50 - i)}))
	}

	query := Vector{1, 0}
	got := g.Search(query, 5)
	if len(got) != 5 {
		t.Fatalf("Search() returned %d nodes, want 5", len(got))
	}
	for i := 1; i < len(got); i++ {
		prev := CosineDistance(got[i-1].Value, query)
		cur := CosineDistance(got[i].Value, query)
		if prev > cur {
			t.Fatalf("cosine results are not sorted: %v > %v at index %d", prev, cur, i)
		}
	}
}

// TestGraphReplaceKeepsHigherLayers is a regression test for Add calling
// Delete(key) inside the insertion loop: replacing a node whose new level was
// higher than the old one wiped the higher layers it had just been inserted
// into.
func TestGraphReplaceKeepsHigherLayers(t *testing.T) {
	g := newTestGraph()

	for i := 0; i < 8; i++ {
		g.Add(MakeNode(i, Vector{float32(i), float32(i % 2)}))
	}

	if len(g.layers) < 3 {
		t.Fatalf("expected a multi-layer graph, got %d layers", len(g.layers))
	}

	// Replacing the node inserts it at every level up to its (new, higher)
	// level: it must be present in all of them.
	g.Add(MakeNode(0, Vector{0.5, 0.5}))

	if g.Len() != 8 {
		t.Fatalf("Len() = %d after replacement, want 8", g.Len())
	}
	for i, layer := range g.layers {
		if _, ok := layer.nodes[0]; !ok {
			t.Fatalf("node 0 is missing from layer %d after being replaced (graph has %d layers)", i, len(g.layers))
		}
	}

	v, ok := g.Lookup(0)
	if !ok {
		t.Fatal("Lookup(0) failed after replacement")
	}
	if v[0] != 0.5 || v[1] != 0.5 {
		t.Fatalf("Lookup(0) = %v, want the replacement vector [0.5 0.5]", v)
	}

	// The replacement must also be findable through the search entry path.
	results := g.Search(Vector{0.5, 0.5}, 1)
	if len(results) != 1 || results[0].Key != 0 {
		t.Fatalf("Search() = %v, want node 0 as the nearest neighbor", results)
	}
}

// TestGraphAddIgnoresStaleEmptyLayers is a regression test for the empty-layer
// branch inserting a node unconditionally: after all nodes are deleted the
// leftover empty layers used to capture the next node regardless of its level.
func TestGraphAddIgnoresStaleEmptyLayers(t *testing.T) {
	g := newTestGraph()

	for i := 0; i < 8; i++ {
		g.Add(MakeNode(i, Vector{float32(i), 0}))
	}
	layersBefore := len(g.layers)
	if layersBefore < 3 {
		t.Fatalf("expected a multi-layer graph, got %d layers", layersBefore)
	}

	for i := 0; i < 8; i++ {
		if !g.Delete(i) {
			t.Fatalf("Delete(%d) reported no deletion", i)
		}
	}
	if g.Len() != 0 {
		t.Fatalf("Len() = %d after deleting everything, want 0", g.Len())
	}

	// A new node with level 0 must only land in the base layer.
	g.Rng = rand.New(halfSource{})
	g.Add(MakeNode(100, Vector{1, 2}))

	if len(g.layers) != layersBefore {
		t.Fatalf("layers changed from %d to %d", layersBefore, len(g.layers))
	}
	if _, ok := g.layers[0].nodes[100]; !ok {
		t.Fatal("node was not inserted into the base layer")
	}
	for i := 1; i < len(g.layers); i++ {
		if _, ok := g.layers[i].nodes[100]; ok {
			t.Fatalf("node holding level 0 was inserted into layer %d", i)
		}
	}
}

// TestGraphDimsOnEmptyGraph is a regression test for Dims dereferencing a nil
// entry node once every node has been deleted.
func TestGraphDimsOnEmptyGraph(t *testing.T) {
	g := newTestGraph()
	g.Add(MakeNode(1, Vector{1, 2, 3}))
	g.Add(MakeNode(2, Vector{4, 5, 6}))

	if got := g.Dims(); got != 3 {
		t.Fatalf("Dims() = %d, want 3", got)
	}

	g.Delete(1)
	g.Delete(2)

	if got := g.Dims(); got != 0 {
		t.Fatalf("Dims() on an empty graph = %d, want 0", got)
	}

	// A graph without any layer reports 0 as well.
	empty := NewGraph[int]()
	if got := empty.Dims(); got != 0 {
		t.Fatalf("Dims() on a fresh graph = %d, want 0", got)
	}

	// Inserting into the emptied graph must work with a new dimensionality.
	g.Add(MakeNode(3, Vector{1, 2, 3, 4}))
	if got := g.Dims(); got != 4 {
		t.Fatalf("Dims() after re-insertion = %d, want 4", got)
	}
}

// TestGraphSearchNonPositiveK guards the make(..., 0, k) capacity in search.
func TestGraphSearchNonPositiveK(t *testing.T) {
	g := newTestGraph()
	g.Add(MakeNode(1, Vector{1, 2}))

	if got := g.Search(Vector{1, 2}, 0); got != nil {
		t.Fatalf("Search with k=0 = %v, want nil", got)
	}
	if got := g.Search(Vector{1, 2}, -1); got != nil {
		t.Fatalf("Search with k=-1 = %v, want nil", got)
	}
}

// TestGraphUsesEfConstruction ensures the construction candidate list size is
// wired into Add instead of being ignored.
func TestGraphUsesEfConstruction(t *testing.T) {
	g := newTestGraph()
	if got := g.constructionEf(); got != 100 {
		t.Fatalf("constructionEf() = %d, want EfConstruction (100)", got)
	}

	g.EfConstruction = 0
	if got := g.constructionEf(); got != g.EfSearch {
		t.Fatalf("constructionEf() = %d, want EfSearch (%d) when EfConstruction is unset", got, g.EfSearch)
	}

	if NewGraph[int]().EfConstruction <= 0 {
		t.Fatal("NewGraph must provide a default EfConstruction")
	}
}

// TestLayerNodeReplenishUsesGraphMetric is a regression test for replenish
// hard-coding the cosine distance: the replacement neighbor must be the closest
// candidate under the graph's own metric.
func TestLayerNodeReplenishUsesGraphMetric(t *testing.T) {
	n := &layerNode[int]{Node: MakeNode(0, Vector{1, 1}), neighbors: map[int]*layerNode[int]{}}
	hub := &layerNode[int]{Node: MakeNode(1, Vector{5, 5}), neighbors: map[int]*layerNode[int]{}}
	// Closest to n under Euclidean distance (1.118), but not under cosine.
	near := &layerNode[int]{Node: MakeNode(2, Vector{2, 0.5}), neighbors: map[int]*layerNode[int]{}}
	// Closest to n under cosine distance (same direction), but far in Euclidean.
	cosineNear := &layerNode[int]{Node: MakeNode(3, Vector{100, 100}), neighbors: map[int]*layerNode[int]{}}

	n.neighbors[hub.Key] = hub
	hub.neighbors[n.Key] = n
	hub.neighbors[near.Key] = near
	hub.neighbors[cosineNear.Key] = cosineNear

	// m = 2: exactly one replacement is picked.
	n.replenish(2, EuclideanDistance)

	if _, ok := n.neighbors[near.Key]; !ok {
		keys := make([]int, 0, len(n.neighbors))
		for k := range n.neighbors {
			keys = append(keys, k)
		}
		t.Fatalf("replenish picked %v, want the closest Euclidean neighbor %d", keys, near.Key)
	}
	if _, ok := n.neighbors[cosineNear.Key]; ok {
		t.Fatal("replenish picked the cosine-closest but Euclidean-farthest neighbor")
	}

	// The same node set with the cosine metric must pick the other candidate,
	// which proves the metric is threaded through instead of hard-coded.
	n2 := &layerNode[int]{Node: MakeNode(10, Vector{1, 1}), neighbors: map[int]*layerNode[int]{}}
	hub2 := &layerNode[int]{Node: MakeNode(11, Vector{5, 5}), neighbors: map[int]*layerNode[int]{}}
	near2 := &layerNode[int]{Node: MakeNode(12, Vector{2, 0.5}), neighbors: map[int]*layerNode[int]{}}
	cosineNear2 := &layerNode[int]{Node: MakeNode(13, Vector{100, 100}), neighbors: map[int]*layerNode[int]{}}

	n2.neighbors[hub2.Key] = hub2
	hub2.neighbors[n2.Key] = n2
	hub2.neighbors[near2.Key] = near2
	hub2.neighbors[cosineNear2.Key] = cosineNear2

	n2.replenish(2, CosineDistance)

	if _, ok := n2.neighbors[cosineNear2.Key]; !ok {
		t.Fatal("replenish with the cosine metric did not pick the cosine-closest neighbor")
	}
}
