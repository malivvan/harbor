package vec

import (
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/malivvan/storm/vec/hnsw"
)

// HNSWParams contains configurable parameters for HNSW index
// See https://github.com/coder/hnsw for more details on these parameters
type HNSWParams struct {
	// M is the maximum number of connections per node
	// Default: 16
	M int

	// EfConstruction is the size of the dynamic candidate list during construction
	// Default: 200
	EfConstruction int

	// EfSearch is the size of the dynamic candidate list during search
	// Default: 64
	EfSearch int

	// K is the number of nearest neighbors to return.
	// Default: 10
	//
	// K is advisory: index searches always return the number of results
	// requested by the caller (the topK argument of Search). The parameter is
	// validated, so a non-positive value is replaced by the default.
	K int
}

// DefaultHNSWParams returns default HNSW parameters
func DefaultHNSWParams() HNSWParams {
	return HNSWParams{
		M:              16,
		EfConstruction: 200,
		EfSearch:       64,
		K:              10,
	}
}

// HNSWIndex wraps the coder/hnsw graph to provide an approximate nearest neighbor search.
// It uses Hierarchical Navigable Small World graphs for efficient similarity search
// with sub-linear complexity, making it suitable for large datasets.
type HNSWIndex struct {
	graph  *hnsw.Graph[string]
	points map[string]*PointStruct // Fast Metadata / Payload lookup
	metric Distance
	params HNSWParams
	mu     sync.RWMutex
}

// NewHNSWIndex creates a new HNSW index engine with the specified distance metric.
// It configures the underlying HNSW graph with appropriate distance functions
// for Cosine, Euclidean, or Dot product metrics.
func NewHNSWIndex(metric Distance) *HNSWIndex {
	return NewHNSWIndexWithParams(metric, DefaultHNSWParams())
}

// normalizeHNSWParams replaces non-positive parameters with their defaults so
// that the graph is never built with degenerate settings.
func normalizeHNSWParams(params HNSWParams) HNSWParams {
	def := DefaultHNSWParams()
	if params.M <= 0 {
		params.M = def.M
	}
	if params.EfConstruction <= 0 {
		params.EfConstruction = def.EfConstruction
	}
	if params.EfSearch <= 0 {
		params.EfSearch = def.EfSearch
	}
	if params.K <= 0 {
		params.K = def.K
	}
	return params
}

// NewHNSWIndexWithParams creates a new HNSW index engine with custom parameters.
// It allows fine-tuning of HNSW parameters for specific use cases: M sizes the
// neighbor lists, EfConstruction is used while inserting nodes, EfSearch while
// querying and K is validated (see HNSWParams.K).
func NewHNSWIndexWithParams(metric Distance, params HNSWParams) *HNSWIndex {
	params = normalizeHNSWParams(params)

	g := hnsw.NewGraph[string]()

	// Configure Distance Function based on our definitions
	switch metric {
	case Euclid:
		g.Distance = func(a, b []float32) float32 {
			var sum float32
			for i := range a {
				diff := a[i] - b[i]
				sum += diff * diff
			}
			return float32(math.Sqrt(float64(sum)))
		}
	case Dot:
		g.Distance = func(a, b []float32) float32 {
			var sum float32
			for i := range a {
				sum += a[i] * b[i]
			}
			// hnsw minimizes distance, so we invert dot product
			return -sum
		}
	case Cosine:
		fallthrough
	default:
		// Default is Cosine Distance (1 - CosineSimilarity)
		// hnsw seeks minimum distance, so 1 - sim is correct
		g.Distance = hnsw.CosineDistance
	}

	// Set HNSW parameters. EfConstruction is what Graph.Add uses to pick the
	// neighbors of a new node; EfSearch keeps its role on the query path.
	g.M = params.M
	g.EfSearch = params.EfSearch
	g.EfConstruction = params.EfConstruction

	return &HNSWIndex{
		graph:  g,
		points: make(map[string]*PointStruct),
		metric: metric,
		params: params,
	}
}

// Upsert adds or updates points in the HNSW graph.
// Points are added to both the HNSW graph for search and a local map for payload lookup.
func (h *HNSWIndex) Upsert(points []PointStruct) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.graph == nil {
		return fmt.Errorf("HNSW graph is nil, cannot upsert points")
	}

	if len(points) == 0 {
		return nil
	}

	// Validate every point against one dimension up front, even for an empty
	// graph: Graph.Add panics when a later node in the same batch disagrees
	// with the first, which would leave the points map inconsistent.
	dims := h.graph.Dims()
	if dims == 0 {
		dims = len(points[0].Vector)
	}
	for i := range points {
		if got := len(points[i].Vector); got != dims {
			return fmt.Errorf("point %s has invalid vector dimension %d (expected %d)", points[i].ID, got, dims)
		}
	}

	// Own deep copies: callers may reuse their vectors/payloads after Upsert.
	stored := make([]PointStruct, len(points))
	nodes := make([]hnsw.Node[string], 0, len(points))
	for i := range points {
		stored[i] = clonePoint(&points[i])
		nodes = append(nodes, hnsw.MakeNode(stored[i].ID, stored[i].Vector))
	}

	// Batch add to graph first: only mirror the points that really made it into
	// the graph, so that Count and Search cannot disagree with the graph.
	h.graph.Add(nodes...)

	for i := range stored {
		h.points[stored[i].ID] = &stored[i]
	}
	return nil
}

// Search performs an approximate nearest neighbor search using the HNSW algorithm.
// It uses a post-filtering strategy: over-fetches results to account for filtered points,
// then applies the payload filter and returns the topK matches.
func (h *HNSWIndex) Search(query []float32, filter *Filter, topK int) ([]ScoredPoint, error) {
	if topK <= 0 {
		return nil, fmt.Errorf("topK must be greater than 0, got %d", topK)
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.graph == nil {
		return nil, fmt.Errorf("HNSW graph is nil, cannot search")
	}
	if dims := h.graph.Dims(); dims > 0 && len(query) != dims {
		return nil, fmt.Errorf("query vector dimension %d does not match the index dimension %d", len(query), dims)
	}
	if len(h.points) == 0 {
		return nil, nil
	}
	if topK > len(h.points) {
		topK = len(h.points)
	}

	// Without a filter the graph result is already the answer.
	if filter == nil {
		return h.collect(query, nil, topK), nil
	}

	// Post-filtering over-fetches. Graph search only visits candidates that
	// are close to the query, so a rare match can sit outside every visited
	// neighbourhood; when the over-fetch does not yield topK matches, fall back
	// to an exact scan so filtered results match the Flat index semantics.
	total := len(h.points)
	fetchK := topK * 10
	if fetchK < topK {
		fetchK = topK
	}
	if fetchK > total {
		fetchK = total
	}
	results := h.collect(query, filter, fetchK)
	if len(results) < topK {
		results = h.scan(query, filter)
	}
	if len(results) > topK {
		results = results[:topK]
	}
	return results, nil
}

// scan evaluates the filter against every indexed point and returns the
// matching points ranked by the configured metric. It is the exact fallback
// used when a filtered graph search cannot fill the requested result set.
func (h *HNSWIndex) scan(query []float32, filter *Filter) []ScoredPoint {
	results := make([]ScoredPoint, 0, len(h.points))
	for _, point := range h.points {
		if !MatchFilter(point.Payload, filter) {
			continue
		}
		results = append(results, ScoredPoint{
			ID:      point.ID,
			Version: point.Version,
			Score:   CalculateDistance(h.metric, query, point.Vector),
			Payload: clonePayload(point.Payload),
		})
	}
	sort.SliceStable(results, func(i, j int) bool {
		if h.metric == Euclid {
			return results[i].Score < results[j].Score
		}
		return results[i].Score > results[j].Score
	})
	return results
}

// collect fetches fetchK graph candidates, applies filter and returns the
// matching points ranked by the configured metric.
func (h *HNSWIndex) collect(query []float32, filter *Filter, fetchK int) []ScoredPoint {
	neighbors := h.graph.Search(query, fetchK)

	results := make([]ScoredPoint, 0, len(neighbors))
	for _, neighbor := range neighbors {
		point, exists := h.points[neighbor.Key]
		if !exists {
			continue
		}
		if !MatchFilter(point.Payload, filter) {
			continue
		}
		results = append(results, ScoredPoint{
			ID:      point.ID,
			Version: point.Version,
			Score:   CalculateDistance(h.metric, query, point.Vector),
			Payload: clonePayload(point.Payload),
		})
	}

	sort.SliceStable(results, func(i, j int) bool {
		if h.metric == Euclid {
			return results[i].Score < results[j].Score
		}
		return results[i].Score > results[j].Score
	})
	return results
}

// Delete removes a point from both the HNSW graph and the local points map.
// Returns an error if the point does not exist in the graph.
func (h *HNSWIndex) Delete(id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.graph == nil {
		return fmt.Errorf("HNSW graph is nil, cannot delete")
	}

	if h.graph.Delete(id) {
		delete(h.points, id)
		return nil
	}
	return fmt.Errorf("point %s not found", id)
}

// Count returns the number of vectors currently stored in the index.
func (h *HNSWIndex) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.points)
}

// GetIDsByFilter returns all point IDs that match the given filter.
func (h *HNSWIndex) GetIDsByFilter(filter *Filter) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var ids []string
	for id, point := range h.points {
		if MatchFilter(point.Payload, filter) {
			ids = append(ids, id)
		}
	}
	return ids
}

// GetPointsByFilter returns all points with full payload matching the given filter.
func (h *HNSWIndex) GetPointsByFilter(filter *Filter) []PointStruct {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var results []PointStruct
	for _, point := range h.points {
		if MatchFilter(point.Payload, filter) {
			results = append(results, clonePoint(point))
		}
	}
	return results
}

// DeleteByFilter removes all points that match the given filter from both
// the HNSW graph and the local points map. Returns the IDs of deleted points.
func (h *HNSWIndex) DeleteByFilter(filter *Filter) ([]string, error) {
	if filter == nil {
		// A nil filter matches everything: deleting on it would wipe the index.
		return nil, fmt.Errorf("filter cannot be nil")
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.graph == nil {
		return nil, fmt.Errorf("HNSW graph is nil, cannot delete by filter")
	}

	var deleted []string
	for id, point := range h.points {
		if MatchFilter(point.Payload, filter) {
			if h.graph.Delete(id) {
				delete(h.points, id)
				deleted = append(deleted, id)
			}
		}
	}
	return deleted, nil
}
