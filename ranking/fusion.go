package ranking

import "sort"

// Fusion selects how several ranked lists are combined.
type Fusion int

const (
	// FusionRRF is reciprocal rank fusion, the default. It uses rank only and
	// is the safe choice when the underlying scores are incomparable.
	FusionRRF Fusion = iota
	// FusionWeighted normalizes each list to [0,1] and sums the weighted
	// scores. Use it when the caller knows the scales are comparable enough
	// for normalization, or when one arm should dominate.
	FusionWeighted
)

// String names the fusion mode.
func (f Fusion) String() string {
	if f == FusionWeighted {
		return "weighted"
	}
	return "rrf"
}

// Fuse combines lists with the requested fusion mode. A zero Fusion selects
// RRF, so callers that only care about the default can leave the field alone.
//
// weights applies to FusionWeighted only; a missing weight is one. k applies
// to FusionRRF only; a non-positive k uses DefaultRRFK.
func Fuse(f Fusion, k int, weights []float64, lists ...[]Result) []Result {
	if f == FusionWeighted {
		return Weighted(weights, lists...)
	}
	return RRF(k, lists...)
}

// Weighted normalizes each list to [0,1] independently, multiplies by the
// corresponding weight (missing weights are 1) and sums the contributions.
// Ties are ordered by ID, so the result is deterministic.
func Weighted(weights []float64, lists ...[]Result) []Result {
	scores := make(map[string]float64)
	for i, list := range lists {
		weight := 1.0
		if i < len(weights) {
			weight = weights[i]
		}
		for id, score := range normalize(list) {
			scores[id] += weight * score
		}
	}

	out := make([]Result, 0, len(scores))
	for id, score := range scores {
		out = append(out, Result{ID: id, Score: score})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// normalize maps a list's scores to [0,1]. When every score is equal, every
// candidate gets 1, so the list still contributes its weight. Duplicate IDs
// inside one list contribute once, matching RRF.
func normalize(list []Result) map[string]float64 {
	if len(list) == 0 {
		return nil
	}
	min, max := list[0].Score, list[0].Score
	for _, item := range list {
		if item.Score < min {
			min = item.Score
		}
		if item.Score > max {
			max = item.Score
		}
	}
	out := make(map[string]float64, len(list))
	seen := make(map[string]struct{}, len(list))
	for _, item := range list {
		if item.ID == "" {
			continue
		}
		if _, ok := seen[item.ID]; ok {
			continue
		}
		seen[item.ID] = struct{}{}
		if max == min {
			out[item.ID] = 1
			continue
		}
		out[item.ID] = (item.Score - min) / (max - min)
	}
	return out
}
