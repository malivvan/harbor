package vector

import (
	"fmt"
	"sort"
)

// SearchOption configures SearchWith.
type SearchOption func(*searchOptions)

// searchOptions holds the resolved SearchWith options.
type searchOptions struct {
	withVectors bool
	scorer      func(query, vector []float32) float64
}

// WithVectors makes SearchWith populate ScoredPoint.Vector with the stored
// embedding of each result.
func WithVectors() SearchOption {
	return func(o *searchOptions) { o.withVectors = true }
}

// WithScorer replaces the final score of each candidate with fn(query, vector).
// Candidate selection still uses the collection's metric, so HNSW recall is
// unchanged; only the ordering of the returned candidates reflects the scorer.
// A higher score is better, regardless of the collection metric.
func WithScorer(fn func(query, vector []float32) float64) SearchOption {
	return func(o *searchOptions) { o.scorer = fn }
}

// SearchWith is Search with per-call options.
//
// The candidate set comes from the same index search as Search, so filters and
// approximate recall are unchanged. WithVectors copies the stored embeddings
// into the results; WithScorer re-scores (and re-orders) the candidates using
// the supplied function.
func (c *Collection) SearchWith(queryVector []float32, filter *Filter, topK int, opts ...SearchOption) ([]ScoredPoint, error) {
	if len(queryVector) != c.VectorLen {
		return nil, fmt.Errorf("query vector length mismatch: expected %d, got %d", c.VectorLen, len(queryVector))
	}
	if topK <= 0 {
		return nil, fmt.Errorf("topK must be greater than 0, got %d", topK)
	}

	var so searchOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&so)
		}
	}
	if !so.withVectors && so.scorer == nil {
		return c.Search(queryVector, filter, topK)
	}

	results, err := c.Search(queryVector, filter, topK)
	if err != nil {
		return nil, err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, ErrClosed
	}
	for i := range results {
		if !so.withVectors && so.scorer == nil {
			continue
		}
		point, err := c.index.Get(results[i].ID)
		if err != nil {
			return nil, err
		}
		if point == nil {
			continue
		}
		if so.withVectors {
			vector := make([]float32, len(point.Vector))
			copy(vector, point.Vector)
			results[i].Vector = vector
		}
		if so.scorer != nil {
			results[i].Score = float32(so.scorer(queryVector, point.Vector))
		}
	}

	if so.scorer != nil {
		sort.Slice(results, func(i, j int) bool {
			if results[i].Score != results[j].Score {
				return results[i].Score > results[j].Score
			}
			return results[i].ID < results[j].ID
		})
	}
	return results, nil
}
