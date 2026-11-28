package graphs

import (
	"context"
	"fmt"
	"math"
	"sort"
)

// PPROptions tunes PersonalizedPageRank.
type PPROptions struct {
	// Damping is the restart probability complement; zero selects 0.85.
	Damping float64
	// Iterations bounds the power iteration; zero selects 30.
	Iterations int
	// EdgeType restricts the walk to one edge type. Empty uses every type.
	EdgeType string
	// Directed keeps edges one-way. The zero value treats the graph as
	// undirected, which is the usual choice for a memory graph.
	Directed bool
}

// PPRResult holds one score per node. Scores sum to 1.
type PPRResult struct {
	Scores map[string]float64
}

// Score returns a node's score, or zero when it was not in the graph.
func (r *PPRResult) Score(id string) float64 {
	if r == nil {
		return 0
	}
	return r.Scores[id]
}

// PersonalizedPageRank runs a restart walk from seeds. An empty seed set
// restarts uniformly over every node.
func (g *Graph) PersonalizedPageRank(ctx context.Context, seeds []string, opts PPROptions) (*PPRResult, error) {
	nodes, edges, err := g.analyticsGraph(ctx, opts.EdgeType)
	if err != nil {
		return nil, err
	}
	damping := opts.Damping
	if damping <= 0 || damping >= 1 {
		damping = 0.85
	}
	iterations := opts.Iterations
	if iterations <= 0 {
		iterations = 30
	}
	n := len(nodes)
	result := &PPRResult{Scores: make(map[string]float64, n)}
	if n == 0 {
		return result, nil
	}

	adj := make(map[string][]string, n)
	for _, edge := range edges {
		adj[edge.From] = append(adj[edge.From], edge.To)
		if !opts.Directed {
			adj[edge.To] = append(adj[edge.To], edge.From)
		}
	}
	for _, node := range nodes {
		if _, ok := adj[node.ID]; !ok {
			adj[node.ID] = nil
		}
	}

	restart := make(map[string]float64, n)
	if len(seeds) == 0 {
		for _, node := range nodes {
			restart[node.ID] = 1 / float64(n)
		}
	} else {
		valid := 0
		for _, seed := range seeds {
			if _, ok := adj[seed]; ok {
				valid++
			}
		}
		if valid == 0 {
			for _, node := range nodes {
				restart[node.ID] = 1 / float64(n)
			}
		} else {
			for _, seed := range seeds {
				if _, ok := adj[seed]; ok {
					restart[seed] = 1 / float64(valid)
				}
			}
		}
	}

	scores := make(map[string]float64, n)
	for id, value := range restart {
		scores[id] = value
	}
	for iter := 0; iter < iterations; iter++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		next := make(map[string]float64, n)
		for id, value := range restart {
			next[id] = (1 - damping) * value
		}
		dangling := 0.0
		for id, value := range scores {
			neighbors := adj[id]
			if len(neighbors) == 0 {
				dangling += value
				continue
			}
			share := damping * value / float64(len(neighbors))
			for _, neighbor := range neighbors {
				next[neighbor] += share
			}
		}
		if dangling > 0 {
			for id, value := range restart {
				next[id] += damping * dangling * value
			}
		}
		scores = next
	}

	total := 0.0
	for _, value := range scores {
		total += value
	}
	if total > 0 {
		for id := range scores {
			scores[id] /= total
		}
	}
	result.Scores = scores
	return result, nil
}

// CommunityOptions tunes Communities.
type CommunityOptions struct {
	// MaxIterations bounds label propagation; zero selects 20.
	MaxIterations int
}

// CommunityResult is one label-propagation partition.
type CommunityResult struct {
	// Communities lists the member IDs, sorted by their first ID.
	Communities [][]string
	// Labels maps a node to its community index.
	Labels map[string]int
	// Modularity is the undirected modularity of the partition.
	Modularity float64
}

// Communities partitions an undirected view of the graph with deterministic
// label propagation. It is deliberately simple and dependency-free; Leiden or
// Louvain can be layered on the same reader later.
func (g *Graph) Communities(ctx context.Context, opts CommunityOptions) (*CommunityResult, error) {
	nodes, edges, err := g.analyticsGraph(ctx, "")
	if err != nil {
		return nil, err
	}
	iterations := opts.MaxIterations
	if iterations <= 0 {
		iterations = 20
	}
	adj := make(map[string][]string, len(nodes))
	for _, edge := range edges {
		adj[edge.From] = append(adj[edge.From], edge.To)
		adj[edge.To] = append(adj[edge.To], edge.From)
	}
	ids := make([]string, 0, len(nodes))
	labels := make(map[string]string, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.ID)
		labels[node.ID] = node.ID
	}
	sort.Strings(ids)

	for iter := 0; iter < iterations; iter++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		changed := false
		for _, id := range ids {
			neighbors := adj[id]
			if len(neighbors) == 0 {
				continue
			}
			counts := make(map[string]int, len(neighbors))
			for _, neighbor := range neighbors {
				counts[labels[neighbor]]++
			}
			best := labels[id]
			bestCount := counts[best]
			for label, count := range counts {
				if count > bestCount || (count == bestCount && label < best) {
					best, bestCount = label, count
				}
			}
			if best != labels[id] {
				labels[id] = best
				changed = true
			}
		}
		if !changed {
			break
		}
	}

	groupIndex := make(map[string]int)
	result := &CommunityResult{Labels: make(map[string]int, len(ids))}
	for _, id := range ids {
		label := labels[id]
		index, ok := groupIndex[label]
		if !ok {
			index = len(result.Communities)
			groupIndex[label] = index
			result.Communities = append(result.Communities, nil)
		}
		result.Communities[index] = append(result.Communities[index], id)
		result.Labels[id] = index
	}
	for _, group := range result.Communities {
		sort.Strings(group)
	}
	sort.Slice(result.Communities, func(i, j int) bool {
		if len(result.Communities[i]) == 0 || len(result.Communities[j]) == 0 {
			return len(result.Communities[i]) > len(result.Communities[j])
		}
		return result.Communities[i][0] < result.Communities[j][0]
	})
	// Reassign labels after sorting so Labels always points at the final slice.
	result.Labels = make(map[string]int, len(ids))
	for i, group := range result.Communities {
		for _, id := range group {
			result.Labels[id] = i
		}
	}
	result.Modularity = modularity(nodes, edges, result.Labels)
	return result, nil
}

// DegreeCentrality returns each node's degree divided by n-1. A graph with one
// node reports zero.
func (g *Graph) DegreeCentrality(ctx context.Context) (map[string]float64, error) {
	nodes, edges, err := g.analyticsGraph(ctx, "")
	if err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(nodes))
	if len(nodes) <= 1 {
		for _, node := range nodes {
			out[node.ID] = 0
		}
		return out, nil
	}
	for _, edge := range edges {
		out[edge.From]++
		out[edge.To]++
	}
	for id := range out {
		out[id] /= float64(len(nodes) - 1)
	}
	for _, node := range nodes {
		if _, ok := out[node.ID]; !ok {
			out[node.ID] = 0
		}
	}
	return out, nil
}

// WeightedPath is a shortest path with a float distance.
type WeightedPath struct {
	Nodes    []*Node
	Edges    []*Edge
	Distance float64
}

// WeightedShortestPath runs Dijkstra over edge weights. An empty weightField
// uses Edge.Weight; a non-empty field reads a numeric property. Non-positive
// weights are treated as 1, and a negative weight is rejected.
func (g *Graph) WeightedShortestPath(ctx context.Context, from, to, weightField string) (*WeightedPath, error) {
	nodes, edges, err := g.analyticsGraph(ctx, "")
	if err != nil {
		return nil, err
	}
	byID := make(map[string]*Node, len(nodes))
	for _, node := range nodes {
		byID[node.ID] = node
	}
	if byID[from] == nil {
		return nil, fmt.Errorf("%w: %q", ErrNodeNotFound, from)
	}
	if byID[to] == nil {
		return nil, fmt.Errorf("%w: %q", ErrNodeNotFound, to)
	}

	type link struct {
		to     string
		weight float64
		edge   *Edge
	}
	adj := make(map[string][]link, len(nodes))
	for _, edge := range edges {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		weight := edge.Weight
		if weightField != "" {
			weight = numericProperty(edge.Properties, weightField)
		}
		if weight < 0 {
			return nil, fmt.Errorf("%w: edge %q has negative weight", ErrInvalidRequest, edge.ID)
		}
		if weight == 0 {
			weight = 1
		}
		adj[edge.From] = append(adj[edge.From], link{to: edge.To, weight: weight, edge: edge})
		adj[edge.To] = append(adj[edge.To], link{to: edge.From, weight: weight, edge: edge})
	}

	dist := make(map[string]float64, len(nodes))
	prev := make(map[string]link, len(nodes))
	visited := make(map[string]bool, len(nodes))
	for _, node := range nodes {
		dist[node.ID] = math.Inf(1)
	}
	dist[from] = 0
	for {
		current := ""
		best := math.Inf(1)
		for _, node := range nodes {
			if !visited[node.ID] && dist[node.ID] < best {
				best = dist[node.ID]
				current = node.ID
			}
		}
		if current == "" {
			break
		}
		if current == to {
			break
		}
		visited[current] = true
		for _, next := range adj[current] {
			if visited[next.to] {
				continue
			}
			candidate := dist[current] + next.weight
			if candidate < dist[next.to] {
				dist[next.to] = candidate
				prev[next.to] = link{to: current, weight: next.weight, edge: next.edge}
			}
		}
	}
	if math.IsInf(dist[to], 1) {
		return nil, fmt.Errorf("%w: no path from %q to %q", ErrNodeNotFound, from, to)
	}

	var path WeightedPath
	for id := to; ; {
		path.Nodes = append(path.Nodes, byID[id])
		step, ok := prev[id]
		if !ok {
			break
		}
		path.Edges = append(path.Edges, step.edge)
		id = step.to
	}
	for i, j := 0, len(path.Nodes)-1; i < j; i, j = i+1, j-1 {
		path.Nodes[i], path.Nodes[j] = path.Nodes[j], path.Nodes[i]
	}
	for i, j := 0, len(path.Edges)-1; i < j; i, j = i+1, j-1 {
		path.Edges[i], path.Edges[j] = path.Edges[j], path.Edges[i]
	}
	path.Distance = dist[to]
	return &path, nil
}

func (g *Graph) analyticsGraph(ctx context.Context, edgeType string) ([]*Node, []*Edge, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, nil, err
	}
	nodes, err := g.GetAllNodes(ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	edges, err := g.GetAllEdges(ctx)
	if err != nil {
		return nil, nil, err
	}
	if edgeType == "" {
		return nodes, edges, nil
	}
	filtered := edges[:0]
	for _, edge := range edges {
		if edge.Type == edgeType {
			filtered = append(filtered, edge)
		}
	}
	return nodes, filtered, nil
}

func modularity(nodes []*Node, edges []*Edge, labels map[string]int) float64 {
	if len(nodes) == 0 || len(edges) == 0 {
		return 0
	}
	degree := make(map[string]float64, len(nodes))
	m2 := 0.0
	for _, edge := range edges {
		degree[edge.From]++
		degree[edge.To]++
		m2 += 2
	}
	if m2 == 0 {
		return 0
	}
	score := 0.0
	for _, edge := range edges {
		if labels[edge.From] == labels[edge.To] {
			score += 1 - degree[edge.From]*degree[edge.To]/m2
		}
	}
	return score / (m2 / 2)
}

func numericProperty(properties map[string]any, key string) float64 {
	value, ok := properties[key]
	if !ok {
		return 0
	}
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case int32:
		return float64(v)
	default:
		return 0
	}
}
