package vec

import (
	"container/heap"
	"fmt"
	"sort"
	"sync"
)

// FlatIndex implements VectorIndex using a brute-force search algorithm.
// It stores all vectors in memory and performs exhaustive distance calculations
// for each query. This provides exact results but has O(n) search complexity,
// making it suitable for small to medium datasets.
type FlatIndex struct {
	points map[string]*PointStruct
	metric Distance
	mu     sync.RWMutex
}

// NewFlatIndex creates a new flat memory index with the specified distance metric.
func NewFlatIndex(metric Distance) *FlatIndex {
	return &FlatIndex{
		points: make(map[string]*PointStruct),
		metric: metric,
	}
}

// Upsert adds or updates points in the index.
// If a point with the same ID already exists, it will be overwritten.
func (f *FlatIndex) Upsert(points []PointStruct) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for i := range points {
		// Own a deep copy: callers may reuse their vectors/payloads after
		// Upsert, and that must not change the index.
		stored := clonePoint(&points[i])
		f.points[stored.ID] = &stored
	}
	return nil
}

// upsertOwned stores points without cloning; the caller transfers ownership.
func (f *FlatIndex) upsertOwned(points []PointStruct) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range points {
		f.points[points[i].ID] = &points[i]
	}
	return nil
}

// Search performs a brute-force search for the nearest neighbors.
// It calculates the distance between the query vector and all stored vectors,
// filters by payload if specified, and returns the topK results.
// Results are sorted by relevance: descending for Cosine/Dot, ascending for Euclidean.
func (f *FlatIndex) Search(query []float32, filter *Filter, topK int) ([]ScoredPoint, error) {
	if topK <= 0 {
		return nil, fmt.Errorf("topK must be greater than 0, got %d", topK)
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	// Keep only the best topK candidates in a max-heap whose root is the
	// current worst, so the scan is O(n log topK) with O(topK) memory.
	h := &flatTopK{euclid: f.metric == Euclid}
	h.data = make([]flatCandidate, 0, topK)

	for id, point := range f.points {
		if !MatchFilter(point.Payload, filter) {
			continue
		}

		c := flatCandidate{
			id:      id,
			version: point.Version,
			dist:    CalculateDistance(f.metric, query, point.Vector),
			point:   point,
		}

		if len(h.data) < topK {
			heap.Push(h, c)
			continue
		}
		if h.better(c, h.data[0]) {
			heap.Pop(h)
			heap.Push(h, c)
		}
	}

	// Order the survivors for the caller.
	sort.Slice(h.data, func(i, j int) bool {
		return h.better(h.data[i], h.data[j])
	})

	// Payloads are deep-copied only for the results that survive topK.
	results := make([]ScoredPoint, len(h.data))
	for i, c := range h.data {
		results[i] = ScoredPoint{
			ID:      c.id,
			Version: c.version,
			Score:   c.dist,
			Payload: clonePayload(c.point.Payload),
		}
	}
	return results, nil
}

// flatCandidate is a lightweight search hit that keeps a reference to the
// stored point so its payload can be cloned only if it survives.
type flatCandidate struct {
	id      string
	version uint64
	dist    float32
	point   *PointStruct
}

// flatTopK is a max-heap of candidates ordered so that the root is the worst
// candidate currently kept.
type flatTopK struct {
	data   []flatCandidate
	euclid bool
}

func (h flatTopK) Len() int { return len(h.data) }

func (h flatTopK) Less(i, j int) bool {
	if h.euclid {
		// Larger Euclidean distance is worse.
		return h.data[i].dist > h.data[j].dist
	}
	// Smaller similarity is worse.
	return h.data[i].dist < h.data[j].dist
}

func (h flatTopK) Swap(i, j int) { h.data[i], h.data[j] = h.data[j], h.data[i] }

func (h *flatTopK) Push(x any) { h.data = append(h.data, x.(flatCandidate)) }

func (h *flatTopK) Pop() any {
	n := len(h.data)
	item := h.data[n-1]
	h.data = h.data[:n-1]
	return item
}

// better reports whether a should rank before b.
func (h *flatTopK) better(a, b flatCandidate) bool {
	if h.euclid {
		return a.dist < b.dist
	}
	return a.dist > b.dist
}

// Delete removes a point from the index by its ID.
// Returns an error if the point does not exist.
func (f *FlatIndex) Delete(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.points[id]; exists {
		delete(f.points, id)
		return nil
	}
	return fmt.Errorf("point %s not found", id)
}

// Count returns the number of vectors currently stored in the index.
func (f *FlatIndex) Count() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.points)
}

// GetIDsByFilter returns all point IDs that match the given filter.
func (f *FlatIndex) GetIDsByFilter(filter *Filter) []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	ids := make([]string, 0, len(f.points))
	for id, point := range f.points {
		if MatchFilter(point.Payload, filter) {
			ids = append(ids, id)
		}
	}
	return ids
}

// GetPointsByFilter returns all points with full payload matching the given filter.
func (f *FlatIndex) GetPointsByFilter(filter *Filter) []PointStruct {
	f.mu.RLock()
	defer f.mu.RUnlock()

	results := make([]PointStruct, 0, len(f.points))
	for _, point := range f.points {
		if MatchFilter(point.Payload, filter) {
			results = append(results, clonePoint(point))
		}
	}
	return results
}

// DeleteByFilter removes all points that match the given filter and returns their IDs.
func (f *FlatIndex) DeleteByFilter(filter *Filter) ([]string, error) {
	if filter == nil {
		// A nil filter matches everything: deleting on it would wipe the index.
		return nil, fmt.Errorf("filter cannot be nil")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	deleted := make([]string, 0, len(f.points))
	for id, point := range f.points {
		if MatchFilter(point.Payload, filter) {
			delete(f.points, id)
			deleted = append(deleted, id)
		}
	}
	return deleted, nil
}
