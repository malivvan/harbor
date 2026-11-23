package hnsw

import (
	"math"
	"sync"
	"testing"
)

// TestCosineDistanceZeroVector is a regression test for zero vectors producing
// NaN cosine distances, which poisons the graph's heap ordering.
func TestCosineDistanceZeroVector(t *testing.T) {
	zero := Vector{0, 0, 0}
	nonZero := Vector{1, 0, 0}

	cases := []struct {
		name string
		a, b Vector
	}{
		{"zero vs nonzero", zero, nonZero},
		{"nonzero vs zero", nonZero, zero},
		{"zero vs zero", zero, Vector{0, 0, 0}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := CosineDistance(tc.a, tc.b)
			if math.IsNaN(float64(got)) {
				t.Fatalf("CosineDistance(%v, %v) = NaN, want a finite value", tc.a, tc.b)
			}
			if got != 1 {
				t.Fatalf("CosineDistance(%v, %v) = %v, want 1", tc.a, tc.b, got)
			}
		})
	}

	// A zero-norm vector must not disturb the ordering of other pairs.
	if got := CosineDistance(nonZero, nonZero); math.Abs(float64(got)) > 1e-6 {
		t.Fatalf("CosineDistance(identical) = %v, want 0", got)
	}
}

// TestDistancesWithMismatchedLengths ensures the distance functions degrade
// gracefully instead of panicking on out-of-range indexing.
func TestDistancesWithMismatchedLengths(t *testing.T) {
	a := Vector{1, 2, 3}
	b := Vector{1, 2}

	if got := EuclideanDistance(a, b); math.IsNaN(float64(got)) {
		t.Fatalf("EuclideanDistance with mismatched lengths = NaN")
	}
	if got := CosineDistance(a, b); math.IsNaN(float64(got)) {
		t.Fatalf("CosineDistance with mismatched lengths = NaN")
	}
}

// TestRegisterDistanceFuncConcurrent exercises the registry from many
// goroutines at once: the map must be guarded (run with -race to verify).
func TestRegisterDistanceFuncConcurrent(t *testing.T) {
	fn := func(a, b []float32) float32 { return 0 }

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			RegisterDistanceFunc("test-distance", fn)
			if _, ok := lookupDistanceFunc("test-distance"); !ok {
				t.Errorf("registered function not found")
			}
			if _, ok := distanceFuncToName(fn); !ok {
				t.Errorf("function is not resolvable to a name")
			}
		}(i)
	}
	wg.Wait()
}
