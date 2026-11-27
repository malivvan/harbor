package vector

import (
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/malivvan/eidos-db/pkg/metrics"
	"github.com/malivvan/eidos-db/store"
	"github.com/malivvan/eidos-db/store/vector/hnsw"
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

	// obs receives fallback and scan-size diagnostics, or nil when the owning
	// store was opened without instrumentation. It is set once at construction.
	obs metrics.Observer
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

// Normalize returns the parameters with non-positive fields replaced by their
// defaults. NewCollectionWithOptions applies it when UseHNSW is set, so callers
// can preview the effective index parameters without creating a collection.
func (p HNSWParams) Normalize() HNSWParams {
	return normalizeHNSWParams(p)
}

// NewHNSWIndexWithParams creates a new HNSW index engine with custom parameters.
// It allows fine-tuning of HNSW parameters for specific use cases: M sizes the
// neighbor lists, EfConstruction is used while inserting nodes, EfSearch while
// querying and K is validated (see HNSWParams.K).
func NewHNSWIndexWithParams(metric Distance, params HNSWParams) *HNSWIndex {
	params = normalizeHNSWParams(params)

	g := hnsw.NewGraph[string]()

	// Configure the distance function once, so the graph's inner loop makes a
	// direct call. Vectors are normalized for cosine, making its distance a
	// single dot product; Euclid ranks by squared distance and only the final
	// top-K scores take the square root.
	switch metric {
	case Euclid:
		g.Distance = euclideanDistanceSquared
	case Dot:
		g.Distance = func(a, b []float32) float32 {
			// hnsw minimizes distance, so we invert the dot product.
			return -dotProduct(a, b)
		}
	case Cosine:
		fallthrough
	default:
		// Vectors are unit length, so 1 - dot is the cosine distance.
		g.Distance = func(a, b []float32) float32 {
			return 1 - dotProduct(a, b)
		}
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
		nodes = append(nodes, hnsw.MakeNode(stored[i].ID, h.graphVector(stored[i].Vector)))
	}

	// Batch add to graph first: only mirror the points that really made it into
	// the graph, so that Count and Search cannot disagree with the graph.
	h.graph.Add(nodes...)

	for i := range stored {
		h.points[stored[i].ID] = &stored[i]
	}
	return nil
}

// upsertOwned stores points without cloning; the caller transfers ownership.
func (h *HNSWIndex) upsertOwned(points []PointStruct) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.graph == nil {
		return fmt.Errorf("HNSW graph is nil, cannot upsert points")
	}
	if len(points) == 0 {
		return nil
	}
	dims := h.graph.Dims()
	if dims == 0 {
		dims = len(points[0].Vector)
	}
	for i := range points {
		if got := len(points[i].Vector); got != dims {
			return fmt.Errorf("point %s has invalid vector dimension %d (expected %d)", points[i].ID, got, dims)
		}
	}
	nodes := make([]hnsw.Node[string], 0, len(points))
	for i := range points {
		nodes = append(nodes, hnsw.MakeNode(points[i].ID, h.graphVector(points[i].Vector)))
	}
	h.graph.Add(nodes...)
	for i := range points {
		h.points[points[i].ID] = &points[i]
	}
	return nil
}

// Search performs an approximate nearest neighbor search using the HNSW algorithm.
// It uses a post-filtering strategy: over-fetches results to account for filtered points,
// then applies the payload filter and returns the topK matches.
//
// The read lock covers only the graph traversal and the point lookups that
// resolve candidate IDs. Payload matching, payload cloning and ranking run
// after it is released, so a slow filter or a large topK does not block
// concurrent upserts for the whole scan.
func (h *HNSWIndex) Search(query []float32, filter *Filter, topK int) ([]ScoredPoint, error) {
	if topK <= 0 {
		return nil, fmt.Errorf("topK must be greater than 0, got %d", topK)
	}

	h.mu.RLock()
	if h.graph == nil {
		h.mu.RUnlock()
		return nil, fmt.Errorf("HNSW graph is nil, cannot search")
	}
	dims := h.graph.Dims()
	total := len(h.points)
	h.mu.RUnlock()

	if dims > 0 && len(query) != dims {
		return nil, fmt.Errorf("query vector dimension %d does not match the index dimension %d", len(query), dims)
	}
	if total == 0 {
		return nil, nil
	}
	if topK > total {
		topK = total
	}

	// Normalize the query once per search rather than once per collect round.
	graphQuery := h.graphVector(query)

	// Without a filter the graph result is already the answer.
	if filter == nil {
		return h.scoreCandidates(h.candidates(graphQuery, topK), nil), nil
	}

	// Post-filtering over-fetches. Graph search only visits candidates close to
	// the query, so a selective filter may need a larger candidate set; grow the
	// fetch geometrically instead of always scanning the whole index. When even
	// the full candidate set cannot prove topK matches, fall back to an exact
	// scan so filtered results match the Flat index semantics.
	fetchK := topK * 4
	if fetchK < topK {
		fetchK = topK
	}
	for {
		if fetchK > total {
			fetchK = total
		}
		results := h.scoreCandidates(h.candidates(graphQuery, fetchK), filter)
		if len(results) >= topK {
			if len(results) > topK {
				results = results[:topK]
			}
			return results, nil
		}
		if fetchK >= total {
			results = h.scan(query, filter)
			h.observeFallback(total)
			if len(results) > topK {
				results = results[:topK]
			}
			return results, nil
		}
		fetchK *= 2
	}
}

// observeFallback reports an exact-scan fallback for a filtered search, with
// the number of points scanned.
func (h *HNSWIndex) observeFallback(scanned int) {
	if h == nil || h.obs == nil {
		return
	}
	labels := metrics.Labels{
		metrics.LabelStore: store.VECTOR,
		metrics.LabelOp:    "search",
	}
	metrics.Counter(h.obs, metrics.MetricVectorFallbacks, 1, labels)
	metrics.Histogram(h.obs, metrics.MetricVectorScanSize, float64(scanned), labels)
}

// graphVector returns the representation stored in the graph. Cosine uses
// unit-length vectors so its distance is a single dot product.
func (h *HNSWIndex) graphVector(v []float32) []float32 {
	if h.metric == Cosine || h.metric == "" {
		return normalizeVector(v)
	}
	return v
}

// scoreFromGraphDistance converts an internal graph distance back to the
// public score used by CalculateDistance.
func (h *HNSWIndex) scoreFromGraphDistance(d float32) float32 {
	switch h.metric {
	case Euclid:
		return float32(math.Sqrt(float64(d)))
	case Dot:
		return -d
	default:
		return 1 - d
	}
}

// normalizeVector returns a unit-length copy of v. A zero vector is returned
// unchanged; its similarity is already well-defined as zero.
func normalizeVector(v []float32) []float32 {
	sum := squaredNormF32(v)
	if sum == 0 {
		return v
	}
	inv := 1 / float32(math.Sqrt(float64(sum)))
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = x * inv
	}
	return out
}

// scan evaluates the filter against every indexed point and returns the
// matching points ranked by the configured metric. It is the exact fallback
// used when a filtered graph search cannot fill the requested result set.
//
// Only the point snapshot is taken under the read lock. Stored records are
// immutable, so distance calculation, filtering and payload cloning run after
// the lock is released.
func (h *HNSWIndex) scan(query []float32, filter *Filter) []ScoredPoint {
	h.mu.RLock()
	points := make([]*PointStruct, 0, len(h.points))
	for _, point := range h.points {
		points = append(points, point)
	}
	h.mu.RUnlock()

	results := make([]ScoredPoint, 0, len(points))
	for _, point := range points {
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

// hnswCandidate is a graph hit with the metadata search needs after the lock
// is released. Stored point records are immutable once published (upserts
// replace the map entry with a new record), so holding the pointer past the
// lock is safe: Delete only removes the map entry and the record stays alive
// through this reference.
type hnswCandidate struct {
	id       string
	distance float32
	point    *PointStruct
}

// candidates runs the graph traversal and resolves the visited IDs to point
// records while holding the read lock. The lock is released before the caller
// filters, clones payloads and ranks, keeping it off the expensive tail of a
// search.
func (h *HNSWIndex) candidates(graphQuery []float32, fetchK int) []hnswCandidate {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.graph == nil {
		return nil
	}
	neighbors := h.graph.SearchWithDistances(graphQuery, fetchK)
	out := make([]hnswCandidate, 0, len(neighbors))
	for _, neighbor := range neighbors {
		point, exists := h.points[neighbor.Node.Key]
		if !exists {
			continue
		}
		out = append(out, hnswCandidate{
			id:       neighbor.Node.Key,
			distance: neighbor.Distance,
			point:    point,
		})
	}
	return out
}

// scoreCandidates applies the payload filter, converts graph distances to
// public scores, clones the surviving payloads and orders the results. It runs
// outside the index lock.
func (h *HNSWIndex) scoreCandidates(candidates []hnswCandidate, filter *Filter) []ScoredPoint {
	results := make([]ScoredPoint, 0, len(candidates))
	for _, candidate := range candidates {
		if !MatchFilter(candidate.point.Payload, filter) {
			continue
		}
		results = append(results, ScoredPoint{
			ID:      candidate.id,
			Version: candidate.point.Version,
			Score:   h.scoreFromGraphDistance(candidate.distance),
			Payload: clonePayload(candidate.point.Payload),
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

	ids := make([]string, 0, len(h.points))
	for id, point := range h.points {
		if MatchFilter(point.Payload, filter) {
			ids = append(ids, id)
		}
	}
	return ids
}

// Get returns a clone of the point with the given ID, or (nil, nil) when it is
// absent.
func (h *HNSWIndex) Get(id string) (*PointStruct, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	point, ok := h.points[id]
	if !ok {
		return nil, nil
	}
	clone := clonePoint(point)
	return &clone, nil
}

// GetPointsByFilter returns all points with full payload matching the given filter.
func (h *HNSWIndex) GetPointsByFilter(filter *Filter) []PointStruct {
	h.mu.RLock()
	defer h.mu.RUnlock()

	results := make([]PointStruct, 0, len(h.points))
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

	deleted := make([]string, 0, len(h.points))
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
