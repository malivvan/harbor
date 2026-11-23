// Package fusion combines ranked result lists.
//
// It is intentionally small: ranking policy, weights and which lists to trust
// stay in the caller. The one helper here is reciprocal rank fusion, which is
// the usual way to merge a full-text result list with a vector result list
// without comparing their incomparable scores.
package fusion

import "sort"

// Result is one candidate in a ranked list.
type Result struct {
	// ID identifies the candidate; the same ID may appear in several lists.
	ID string
	// Score is the score reported by the producing search. RRF ignores it:
	// fusion is by rank, because a BM25 score and a cosine similarity cannot be
	// added meaningfully.
	Score float64
}

// DefaultRRFK is the constant used by RRF when k is non-positive. The usual
// literature value is 60.
const DefaultRRFK = 60

// RRF fuses ranked lists with reciprocal rank fusion: a candidate at 1-based
// rank r in a list contributes 1/(k+r), and contributions are summed across
// lists. A higher fused Score is better. The result is ordered by fused score
// descending and then by ID, so the output is deterministic.
//
// k damps the influence of top ranks; a non-positive k uses DefaultRRFK. Lists
// may be empty; the returned slice is empty when every list is empty.
func RRF(k int, lists ...[]Result) []Result {
	if k <= 0 {
		k = DefaultRRFK
	}
	scores := make(map[string]float64)
	for _, list := range lists {
		seen := make(map[string]struct{}, len(list))
		for i, item := range list {
			if item.ID == "" {
				continue
			}
			if _, ok := seen[item.ID]; ok {
				// A duplicated ID inside one list must not contribute twice.
				continue
			}
			seen[item.ID] = struct{}{}
			scores[item.ID] += 1 / float64(k+i+1)
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
