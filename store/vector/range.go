package vector

import (
	"fmt"
	"sort"
)

// RangeSearch returns every point within radius of query, best first.
//
// radius is a distance in the collection's metric space: Euclidean distance,
// 1 - cosine similarity, or -dot product. Results carry the same Score the
// collection's Search returns (cosine similarity, dot product, or Euclidean
// distance), so a caller can rank them exactly as usual. A negative radius is
// an error; zero returns points whose distance is exactly zero.
//
// The scan is exact for both index types: it evaluates the filter against every
// indexed point instead of walking the HNSW graph. That is the documented
// trade-off for an unbounded radius, where an approximate graph walk cannot
// bound its result set.
func (c *Collection) RangeSearch(query []float32, radius float32, filter *Filter) ([]ScoredPoint, error) {
	return c.RangeSearchN(query, radius, filter, 0)
}

// RangeSearchN is RangeSearch capped at limit results. A non-positive limit
// returns every match.
func (c *Collection) RangeSearchN(query []float32, radius float32, filter *Filter, limit int) ([]ScoredPoint, error) {
	if len(query) != c.VectorLen {
		return nil, fmt.Errorf("query vector length mismatch: expected %d, got %d", c.VectorLen, len(query))
	}
	if radius < 0 {
		return nil, fmt.Errorf("%w: radius must not be negative, got %v", ErrInvalidOptions, radius)
	}
	if limit < 0 {
		return nil, fmt.Errorf("%w: limit must not be negative, got %d", ErrInvalidOptions, limit)
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, ErrClosed
	}

	points := c.index.GetPointsByFilter(filter)
	hits := make([]ScoredPoint, 0, len(points))
	for _, point := range points {
		if len(point.Vector) != len(query) {
			continue
		}
		if pointExpired(&point, c.now()) {
			continue
		}

		var distance, score float32
		switch c.Metric {
		case Euclid:
			score = CalculateDistance(Euclid, query, point.Vector)
			distance = score
		case Dot:
			score = CalculateDistance(Dot, query, point.Vector)
			distance = -score
		default: // Cosine
			score = CalculateDistance(Cosine, query, point.Vector)
			distance = 1 - score
		}
		if distance > radius {
			continue
		}
		hits = append(hits, ScoredPoint{
			ID:      point.ID,
			Version: point.Version,
			Score:   score,
			Payload: point.Payload,
		})
	}

	euclid := c.Metric == Euclid
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			if euclid {
				return hits[i].Score < hits[j].Score
			}
			return hits[i].Score > hits[j].Score
		}
		return hits[i].ID < hits[j].ID
	})
	if limit > 0 && len(hits) > limit {
		hits = hits[:limit]
	}
	return hits, nil
}
