package hnsw

import (
	"math"
	"reflect"
	"sync"
)

// DistanceFunc is a function that computes the distance between two vectors.
type DistanceFunc func(a, b []float32) float32

// CosineDistance computes the cosine distance between two vectors.
//
// The distance is 1 - cosineSimilarity. When either vector has a zero norm the
// similarity is undefined (0/0): instead of returning NaN, which would violate
// the ordering invariants of the graph heap, a distance of 1.0 is returned.
func CosineDistance(a, b []float32) float32 {
	var dot, normA, normB float32
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		// A zero vector has no direction: define its distance as the maximum
		// cosine distance instead of letting NaN poison the graph.
		return 1
	}
	return 1 - dot/float32(math.Sqrt(float64(normA))*math.Sqrt(float64(normB)))
}

// EuclideanDistance computes the Euclidean distance between two vectors.
func EuclideanDistance(a, b []float32) float32 {
	// TODO: can we speedup with vek?
	var sum float32 = 0
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return float32(math.Sqrt(float64(sum)))
}

var (
	// distanceFuncsMu guards distanceFuncs, which is written by
	// RegisterDistanceFunc and read when exporting and importing graphs.
	distanceFuncsMu sync.RWMutex
	distanceFuncs   = map[string]DistanceFunc{
		"euclidean": EuclideanDistance,
		"cosine":    CosineDistance,
	}
)

// lookupDistanceFunc returns the distance function registered under name.
func lookupDistanceFunc(name string) (DistanceFunc, bool) {
	distanceFuncsMu.RLock()
	defer distanceFuncsMu.RUnlock()

	fn, ok := distanceFuncs[name]
	return fn, ok
}

func distanceFuncToName(fn DistanceFunc) (string, bool) {
	distanceFuncsMu.RLock()
	defer distanceFuncsMu.RUnlock()

	fnptr := reflect.ValueOf(fn).Pointer()
	for name, f := range distanceFuncs {
		fptr := reflect.ValueOf(f).Pointer()
		if fptr == fnptr {
			return name, true
		}
	}
	return "", false
}

// RegisterDistanceFunc registers a distance function with a name.
// A distance function must be registered here before a graph can be
// exported and imported. It is safe to call concurrently.
func RegisterDistanceFunc(name string, fn DistanceFunc) {
	distanceFuncsMu.Lock()
	defer distanceFuncsMu.Unlock()

	distanceFuncs[name] = fn
}
