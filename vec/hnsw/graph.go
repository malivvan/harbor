package hnsw

import (
	"cmp"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"time"

	"github.com/malivvan/storm/vec/hnsw/heap"
)

type Vector = []float32

// Node is a node in the graph.
type Node[K cmp.Ordered] struct {
	Key   K
	Value Vector
}

func MakeNode[K cmp.Ordered](key K, vec Vector) Node[K] {
	return Node[K]{Key: key, Value: vec}
}

// layerNode is a node in a layer of the graph.
type layerNode[K cmp.Ordered] struct {
	Node[K]

	// neighbors is map of neighbor keys to neighbor nodes.
	// It is a map and not a slice to allow for efficient deletes, esp.
	// when M is high.
	neighbors map[K]*layerNode[K]
}

// addNeighbor adds a o neighbor to the node, replacing the neighbor
// with the worst distance if the neighbor set is full.
func (n *layerNode[K]) addNeighbor(newNode *layerNode[K], m int, dist DistanceFunc) {
	if n.neighbors == nil {
		n.neighbors = make(map[K]*layerNode[K], m)
	}

	n.neighbors[newNode.Key] = newNode
	if len(n.neighbors) <= m {
		return
	}

	// Find the neighbor with the worst distance.
	var (
		worstDist = float32(math.Inf(-1))
		worst     *layerNode[K]
	)
	for _, neighbor := range n.neighbors {
		d := dist(neighbor.Value, n.Value)
		// d > worstDist may always be false if the distance function
		// returns NaN, e.g., when the embeddings are zero.
		if d > worstDist || worst == nil {
			worstDist = d
			worst = neighbor
		}
	}

	delete(n.neighbors, worst.Key)
	// Delete backlink from the worst neighbor.
	delete(worst.neighbors, n.Key)
	worst.replenish(m, dist)
}

type searchCandidate[K cmp.Ordered] struct {
	node *layerNode[K]
	dist float32
}

func (s searchCandidate[K]) Less(o searchCandidate[K]) bool {
	return s.dist < o.dist
}

// search returns the layer node closest to the target node
// within the same layer.
//
// The returned candidates are ordered by ascending distance, so callers can
// rely on index 0 being the nearest neighbor and the last element being the
// farthest of the retained set.
func (n *layerNode[K]) search(
	// k is the number of candidates in the result set.
	k int,
	efSearch int,
	target Vector,
	distance DistanceFunc,
) []searchCandidate[K] {
	// This is a basic greedy algorithm to find the entry point at the given level
	// that is closest to the target node.
	candidates := heap.Heap[searchCandidate[K]]{}
	candidates.Init(make([]searchCandidate[K], 0, efSearch))
	candidates.Push(
		searchCandidate[K]{
			node: n,
			dist: distance(n.Value, target),
		},
	)
	var (
		result  = heap.Heap[searchCandidate[K]]{}
		visited = make(map[K]bool)
	)
	result.Init(make([]searchCandidate[K], 0, k))

	// Begin with the entry node in the result set.
	result.Push(candidates.Min())
	visited[n.Key] = true

	for candidates.Len() > 0 {
		// Standard HNSW termination: stop once the closest unexplored
		// candidate is farther away than the worst retained result.
		if result.Len() >= k && candidates.Min().dist > result.Max().dist {
			break
		}

		current := candidates.Pop().node

		// We iterate the map in a sorted, deterministic fashion for
		// tests.
		neighborKeys := mapsKeys(current.neighbors)
		slices.Sort(neighborKeys)
		for _, neighborID := range neighborKeys {
			neighbor := current.neighbors[neighborID]
			if visited[neighborID] {
				continue
			}
			visited[neighborID] = true

			dist := distance(neighbor.Value, target)
			if result.Len() < k {
				result.Push(searchCandidate[K]{node: neighbor, dist: dist})
			} else if dist < result.Max().dist {
				// Evict the farthest candidate, not an arbitrary heap leaf.
				result.PopLast()
				result.Push(searchCandidate[K]{node: neighbor, dist: dist})
			}

			candidates.Push(searchCandidate[K]{node: neighbor, dist: dist})
			// Always store candidates if we haven't reached the limit.
			if candidates.Len() > efSearch {
				candidates.PopLast()
			}
		}
	}

	// The result set is a min-heap: only its root is guaranteed to be the
	// nearest candidate. Sort it so callers get a properly ranked list.
	out := result.Slice()
	slices.SortStableFunc(out, func(a, b searchCandidate[K]) int {
		switch {
		case a.dist < b.dist:
			return -1
		case a.dist > b.dist:
			return 1
		default:
			return 0
		}
	})
	return out
}

// replenish restores connectivity for a node that lost a neighbor, using the
// same distance function as the graph so that the replacement edges are ranked
// with the graph's metric instead of an arbitrary one.
func (n *layerNode[K]) replenish(m int, dist DistanceFunc) {
	if len(n.neighbors) >= m {
		return
	}

	// Gather the second-degree candidates, ordered by their distance to n so
	// that the closest replacements are reconnected first.
	type candidate struct {
		node *layerNode[K]
		dist float32
	}

	candidates := make([]candidate, 0, m)
	seen := make(map[K]bool, len(n.neighbors))
	for key := range n.neighbors {
		// do not add duplicates
		seen[key] = true
	}

	for _, neighbor := range n.neighbors {
		for key, c := range neighbor.neighbors {
			if seen[key] {
				continue
			}
			if c == nil || c == n {
				continue
			}
			seen[key] = true
			candidates = append(candidates, candidate{node: c, dist: dist(c.Value, n.Value)})
		}
	}

	slices.SortStableFunc(candidates, func(a, b candidate) int {
		switch {
		case a.dist < b.dist:
			return -1
		case a.dist > b.dist:
			return 1
		default:
			return 0
		}
	})

	for _, c := range candidates {
		if len(n.neighbors) >= m {
			return
		}
		n.addNeighbor(c.node, m, dist)
	}
}

// isolates remove the node from the graph by removing all connections
// to neighbors.
func (n *layerNode[K]) isolate(m int, dist DistanceFunc) {
	for _, neighbor := range n.neighbors {
		delete(neighbor.neighbors, n.Key)
		neighbor.replenish(m, dist)
	}
}

type layer[K cmp.Ordered] struct {
	// nodes is a map of nodes IDs to nodes.
	// All nodes in a higher layer are also in the lower layers, an essential
	// property of the graph.
	//
	// nodes is exported for interop with encoding/gob.
	nodes map[K]*layerNode[K]
}

// entry returns the entry node of the layer.
// It doesn't matter which node is returned, even that the
// entry node is consistent, so we just return the first node
// in the map to avoid tracking extra state.
func (l *layer[K]) entry() *layerNode[K] {
	if l == nil {
		return nil
	}
	for _, node := range l.nodes {
		return node
	}
	return nil
}

func (l *layer[K]) size() int {
	if l == nil {
		return 0
	}
	return len(l.nodes)
}

// Graph is a Hierarchical Navigable Small World graph.
// All public parameters must be set before adding nodes to the graph.
// K is cmp.Ordered instead of of comparable so that they can be sorted.
type Graph[K cmp.Ordered] struct {
	// Distance is the distance function used to compare embeddings.
	Distance DistanceFunc

	// Rng is used for level generation. It may be set to a deterministic value
	// for reproducibility. Note that deterministic number generation can lead to
	// degenerate graphs when exposed to adversarial inputs.
	Rng *rand.Rand

	// M is the maximum number of neighbors to keep for each node.
	// A good default for OpenAI embeddings is 16.
	M int

	// Ml is the level generation factor.
	// E.g., for Ml = 0.25, each layer is 1/4 the size of the previous layer.
	Ml float64

	// EfSearch is the number of nodes to consider in the search phase.
	// 20 is a reasonable default. Higher values improve search accuracy at
	// the expense of memory.
	EfSearch int

	// EfConstruction is the number of nodes to consider while connecting a
	// newly inserted node. When zero, EfSearch is used instead. Higher values
	// produce better connected graphs at the expense of insertion time.
	EfConstruction int

	// layers is a slice of layers in the graph.
	layers []*layer[K]
}

func defaultRand() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

// NewGraph returns a new graph with default parameters, roughly designed for
// storing OpenAI embeddings.
func NewGraph[K cmp.Ordered]() *Graph[K] {
	return &Graph[K]{
		M:              16,
		Ml:             0.25,
		Distance:       CosineDistance,
		EfSearch:       20,
		EfConstruction: 200,
		Rng:            defaultRand(),
	}
}

// constructionEf returns the candidate-list size used while inserting nodes.
func (g *Graph[K]) constructionEf() int {
	if g.EfConstruction > 0 {
		return g.EfConstruction
	}
	if g.EfSearch > 0 {
		return g.EfSearch
	}
	return 20
}

// maxLevel returns an upper-bound on the number of levels in the graph
// based on the size of the base layer.
func maxLevel(ml float64, numNodes int) int {
	if ml == 0 {
		panic("ml must be greater than 0")
	}

	if numNodes == 0 {
		return 1
	}

	l := math.Log(float64(numNodes))
	l /= math.Log(1 / ml)

	m := int(math.Round(l)) + 1

	return m
}

// randomLevel generates a random level for a new node.
func (h *Graph[K]) randomLevel() int {
	// max avoids having to accept an additional parameter for the maximum level
	// by calculating a probably good one from the size of the base layer.
	max := 1
	if len(h.layers) > 0 {
		if h.Ml == 0 {
			panic("(*Graph).Ml must be greater than 0")
		}
		max = maxLevel(h.Ml, h.layers[0].size())
	}

	for level := 0; level < max; level++ {
		if h.Rng == nil {
			h.Rng = defaultRand()
		}
		r := h.Rng.Float64()
		if r > h.Ml {
			return level
		}
	}

	return max
}

func (g *Graph[K]) assertDims(n Vector) {
	if len(g.layers) == 0 {
		return
	}
	// 图可能因 Remove 删空而存在但无节点：此时无可参考的维度基准，
	// 跳过检查——即将插入的节点会重新建立维度基准。
	entry := g.layers[0].entry()
	if entry == nil {
		return
	}
	if hasDims := len(entry.Value); hasDims != len(n) {
		panic(fmt.Sprint("embedding dimension mismatch: ", hasDims, " != ", len(n)))
	}
}

// Dims returns the number of dimensions in the graph, or
// 0 if the graph is empty.
func (g *Graph[K]) Dims() int {
	if len(g.layers) == 0 {
		return 0
	}
	entry := g.layers[0].entry()
	if entry == nil {
		// The base layer may be non-nil but empty (all nodes deleted), in
		// which case there is no dimension reference to report.
		return 0
	}
	return len(entry.Value)
}

func ptr[T any](v T) *T {
	return &v
}

// Add inserts nodes into the graph.
// If another node with the same ID exists, it is replaced.
func (g *Graph[K]) Add(nodes ...Node[K]) {
	for _, node := range nodes {
		key := node.Key
		vec := node.Value

		g.assertDims(vec)

		// Remove an existing node with the same key from every layer exactly
		// once, before the insertion loop. Doing this from inside the loop
		// would drop the higher layers that were just created for the new node
		// whenever the new level exceeds the level of the node being replaced.
		g.remove(key)

		insertLevel := g.randomLevel()
		// Create layers that don't exist yet.
		for insertLevel >= len(g.layers) {
			g.layers = append(g.layers, &layer[K]{})
		}

		if insertLevel < 0 {
			panic("invalid level")
		}

		var elevator *K

		preLen := g.Len()
		efConstruction := g.constructionEf()

		// Insert node at each layer, beginning with the highest.
		for i := len(g.layers) - 1; i >= 0; i-- {
			layer := g.layers[i]
			newNode := &layerNode[K]{
				Node: Node[K]{
					Key:   key,
					Value: vec,
				},
			}

			// Insert the new node into the layer.
			if layer.entry() == nil {
				// Only host the new node when its level covers this layer:
				// layers may be left empty by deletions, and inserting there
				// unconditionally would place nodes above their assigned level.
				if insertLevel >= i {
					if layer.nodes == nil {
						layer.nodes = make(map[K]*layerNode[K], 1)
					}
					layer.nodes[key] = newNode
				}
				continue
			}

			// Now at the highest layer with more than one node, so we can begin
			// searching for the best way to enter the graph.
			searchPoint := layer.entry()

			// On subsequent layers, we use the elevator node to enter the graph
			// at the best point.
			if elevator != nil {
				if ep, ok := layer.nodes[*elevator]; ok {
					searchPoint = ep
				} else {
					// Replace 场景（高层 g.Delete 已删除旧节点、新节点尚未在本层
					// 重插）下 elevator 会悬空，回退到本层入口节点。
					searchPoint = layer.entry()
				}
			}

			if g.Distance == nil {
				panic("(*Graph).Distance must be set")
			}

			neighborhood := searchPoint.search(g.M, efConstruction, vec, g.Distance)
			if len(neighborhood) == 0 {
				// This should never happen because the searchPoint itself
				// should be in the result set.
				panic("no nodes found")
			}

			// Re-set the elevator node for the next layer.
			elevator = ptr(neighborhood[0].node.Key)

			if insertLevel >= i {
				// Insert the new node into the layer.
				layer.nodes[key] = newNode
				for _, node := range neighborhood {
					// Create a bi-directional edge between the new node and the best node.
					node.node.addNeighbor(newNode, g.M, g.Distance)
					newNode.addNeighbor(node.node, g.M, g.Distance)
				}
			}
		}

		// Invariant check: a node was removed first (for replacements) and one
		// node is added, so the graph must have grown by exactly one node.
		if g.Len() != preLen+1 {
			panic("node not added")
		}
	}
}

// Search finds the k nearest neighbors from the target node.
// The returned nodes are ordered by ascending distance according to the
// graph's Distance function.
func (h *Graph[K]) Search(near Vector, k int) []Node[K] {
	if k <= 0 {
		return nil
	}

	h.assertDims(near)
	if len(h.layers) == 0 {
		return nil
	}

	var (
		efSearch = h.EfSearch

		elevator *K
	)

	for layer := len(h.layers) - 1; layer >= 0; layer-- {
		searchPoint := h.layers[layer].entry()
		if searchPoint == nil {
			// 空层：可能是替换删除（Add 覆盖同 key）后残留的高层，或图已被删空。
			// 底层为空意味着图中已无任何节点，直接返回空结果。
			if layer == 0 {
				return nil
			}
			continue
		}

		// elevator 可能悬空（本层节点在替换场景中被删除、新节点尚未重插），
		// 回退到本层入口节点。
		if elevator != nil {
			if ep, ok := h.layers[layer].nodes[*elevator]; ok {
				searchPoint = ep
			} else {
				searchPoint = h.layers[layer].entry()
			}
		}

		// Descending hierarchies
		if layer > 0 {
			nodes := searchPoint.search(1, efSearch, near, h.Distance)
			if len(nodes) == 0 {
				// 防御：理论上 search 至少返回入口节点自身。
				continue
			}
			elevator = ptr(nodes[0].node.Key)
			continue
		}

		nodes := searchPoint.search(k, efSearch, near, h.Distance)
		out := make([]Node[K], 0, len(nodes))

		for _, node := range nodes {
			out = append(out, node.node.Node)
		}

		return out
	}

	panic("unreachable")
}

// Len returns the number of nodes in the graph.
func (h *Graph[K]) Len() int {
	if len(h.layers) == 0 {
		return 0
	}
	return h.layers[0].size()
}

// Delete removes a node from the graph by key.
// It tries to preserve the clustering properties of the graph by
// replenishing connectivity in the affected neighborhoods.
func (h *Graph[K]) Delete(key K) bool {
	if len(h.layers) == 0 {
		return false
	}
	return h.remove(key)
}

// remove deletes the node with the given key from every layer, repairing the
// connectivity of its former neighbors, and reports whether a node was found.
func (h *Graph[K]) remove(key K) bool {
	var (
		deleted bool
		dist    = h.Distance
	)
	if dist == nil {
		dist = CosineDistance
	}

	for _, layer := range h.layers {
		node, ok := layer.nodes[key]
		if !ok {
			continue
		}
		delete(layer.nodes, key)
		if node != nil {
			node.isolate(h.M, dist)
		}
		deleted = true
	}

	return deleted
}

// Lookup returns the vector with the given key.
func (h *Graph[K]) Lookup(key K) (Vector, bool) {
	if len(h.layers) == 0 {
		return nil, false
	}

	node, ok := h.layers[0].nodes[key]
	if !ok {
		return nil, false
	}
	return node.Value, ok
}
