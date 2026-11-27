// Package evals provides dependency-free retrieval metrics and seed-stable
// synthetic corpora for tests and benchmarks.
//
// It is not part of the runtime hot path. Its purpose is a regression floor:
// an analyzer, fusion weight, HNSW parameter or distance-kernel change that
// lowers recall should fail a test instead of shipping silently.
package evals

import "math"

// RecallAtK returns the fraction of the exact top-k IDs that also appear in
// the approximate top-k. It is 0 when either list or k is empty.
func RecallAtK(exact, approx []string, k int) float64 {
	if k <= 0 || len(exact) == 0 {
		return 0
	}
	if k > len(exact) {
		k = len(exact)
	}
	exactSet := make(map[string]struct{}, k)
	for i := 0; i < k; i++ {
		exactSet[exact[i]] = struct{}{}
	}
	hits := 0
	seen := make(map[string]struct{}, k)
	for _, id := range approx {
		if len(seen) >= k {
			break
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		if _, ok := exactSet[id]; ok {
			hits++
		}
	}
	return float64(hits) / float64(k)
}

// NDCG returns normalized discounted cumulative gain at k. Grades are
// non-negative integers; an absent item has grade 0. It returns 0 when the
// ideal DCG is 0.
func NDCG(grades map[string]int, ranked []string, k int) float64 {
	if k <= 0 || len(grades) == 0 {
		return 0
	}
	dcg := 0.0
	for i := 0; i < len(ranked) && i < k; i++ {
		g := grades[ranked[i]]
		if g <= 0 {
			continue
		}
		dcg += (math.Pow(2, float64(g)) - 1) / math.Log2(float64(i)+2)
	}
	ideal := make([]int, 0, len(grades))
	for _, g := range grades {
		if g > 0 {
			ideal = append(ideal, g)
		}
	}
	sortIntsDesc(ideal)
	idcg := 0.0
	for i := 0; i < len(ideal) && i < k; i++ {
		idcg += (math.Pow(2, float64(ideal[i])) - 1) / math.Log2(float64(i)+2)
	}
	if idcg == 0 {
		return 0
	}
	return dcg / idcg
}

// MRR returns the reciprocal rank of the first relevant item, or 0 when no
// ranked item is relevant.
func MRR(ranked []string, relevant map[string]bool) float64 {
	for i, id := range ranked {
		if relevant[id] {
			return 1 / float64(i+1)
		}
	}
	return 0
}

func sortIntsDesc(v []int) {
	for i := 1; i < len(v); i++ {
		for j := i; j > 0 && v[j] > v[j-1]; j-- {
			v[j], v[j-1] = v[j-1], v[j]
		}
	}
}
