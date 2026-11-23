package vec

import (
	"math"
	"testing"
)

func TestCalculateDistance(t *testing.T) {
	// Test Cosine similarity
	t.Run("CosineSimilarity", func(t *testing.T) {
		// Identical vectors should have cosine similarity 1.0
		a := []float32{1.0, 0.0, 0.0}
		b := []float32{1.0, 0.0, 0.0}
		similarity := CalculateDistance(Cosine, a, b)
		if math.Abs(float64(similarity-1.0)) > 1e-6 {
			t.Errorf("Expected cosine similarity 1.0, got %f", similarity)
		}

		// Orthogonal vectors should have cosine similarity 0.0
		a = []float32{1.0, 0.0, 0.0}
		b = []float32{0.0, 1.0, 0.0}
		similarity = CalculateDistance(Cosine, a, b)
		if math.Abs(float64(similarity)) > 1e-6 {
			t.Errorf("Expected cosine similarity 0.0, got %f", similarity)
		}

		// Opposite vectors should have cosine similarity -1.0
		a = []float32{1.0, 0.0, 0.0}
		b = []float32{-1.0, 0.0, 0.0}
		similarity = CalculateDistance(Cosine, a, b)
		if math.Abs(float64(similarity+1.0)) > 1e-6 {
			t.Errorf("Expected cosine similarity -1.0, got %f", similarity)
		}

		// Test with zero vectors
		a = []float32{0.0, 0.0, 0.0}
		b = []float32{1.0, 0.0, 0.0}
		similarity = CalculateDistance(Cosine, a, b)
		if similarity != 0.0 {
			t.Errorf("Expected cosine similarity 0.0 for zero vector, got %f", similarity)
		}
	})

	// Test Euclidean distance
	t.Run("EuclideanDistance", func(t *testing.T) {
		// Identical vectors should have distance 0.0
		a := []float32{1.0, 2.0, 3.0}
		b := []float32{1.0, 2.0, 3.0}
		distance := CalculateDistance(Euclid, a, b)
		if math.Abs(float64(distance)) > 1e-6 {
			t.Errorf("Expected Euclidean distance 0.0, got %f", distance)
		}

		// Test distance between (0,0) and (3,4) should be 5.0
		a = []float32{0.0, 0.0}
		b = []float32{3.0, 4.0}
		distance = CalculateDistance(Euclid, a, b)
		if math.Abs(float64(distance-5.0)) > 1e-6 {
			t.Errorf("Expected Euclidean distance 5.0, got %f", distance)
		}
	})

	// Test Dot product
	t.Run("DotProduct", func(t *testing.T) {
		// Test dot product of [1,2,3] and [4,5,6] = 1*4 + 2*5 + 3*6 = 4+10+18=32
		a := []float32{1.0, 2.0, 3.0}
		b := []float32{4.0, 5.0, 6.0}
		product := CalculateDistance(Dot, a, b)
		if math.Abs(float64(product-32.0)) > 1e-6 {
			t.Errorf("Expected dot product 32.0, got %f", product)
		}

		// Test dot product of orthogonal vectors should be 0.0
		a = []float32{1.0, 0.0, 0.0}
		b = []float32{0.0, 1.0, 0.0}
		product = CalculateDistance(Dot, a, b)
		if math.Abs(float64(product)) > 1e-6 {
			t.Errorf("Expected dot product 0.0 for orthogonal vectors, got %f", product)
		}
	})

	// Test default metric (should be Cosine)
	t.Run("DefaultMetric", func(t *testing.T) {
		a := []float32{1.0, 0.0, 0.0}
		b := []float32{1.0, 0.0, 0.0}
		similarity := CalculateDistance("Unknown", a, b)
		if math.Abs(float64(similarity-1.0)) > 1e-6 {
			t.Errorf("Expected default metric (Cosine) similarity 1.0, got %f", similarity)
		}
	})
}

// referenceDot and referenceL2 are plain scalar implementations used to check
// the build-tagged kernels, including their unrolled tails.
func referenceDot(a, b []float32) float32 {
	var sum float32
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}

func referenceL2(a, b []float32) float32 {
	var sum float32
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}

func kernelVectors(n int) ([]float32, []float32) {
	a := make([]float32, n)
	b := make([]float32, n)
	for i := 0; i < n; i++ {
		a[i] = float32(i%13-6) / 7
		b[i] = float32((i*5)%11-5) / 3
	}
	return a, b
}

// TestDistanceKernels checks the architecture-specific kernels against scalar
// references across the unrolled width, so a tail or off-by-one in the amd64
// build cannot pass unnoticed.
func TestDistanceKernels(t *testing.T) {
	for n := 0; n <= 17; n++ {
		a, b := kernelVectors(n)
		if got, want := dotF32(a, b), referenceDot(a, b); math.Abs(float64(got-want)) > 1e-5 {
			t.Fatalf("dotF32 len=%d: got %f, want %f", n, got, want)
		}
		if got, want := l2SquaredF32(a, b), referenceL2(a, b); math.Abs(float64(got-want)) > 1e-5 {
			t.Fatalf("l2SquaredF32 len=%d: got %f, want %f", n, got, want)
		}
	}
}

func TestCosineWithNorm(t *testing.T) {
	for n := 1; n <= 17; n++ {
		a, b := kernelVectors(n)
		normA := squaredNormF32(a)
		want := cosineSimilarity(a, b)
		got := cosineWithNorm(a, b, normA)
		if math.Abs(float64(got-want)) > 1e-5 {
			t.Fatalf("cosineWithNorm len=%d: got %f, want %f", n, got, want)
		}
	}

	// A zero norm still yields the neutral similarity instead of dividing by
	// zero.
	if got := cosineWithNorm([]float32{0, 0}, []float32{1, 0}, 0); got != 0 {
		t.Fatalf("cosineWithNorm zero norm: got %f, want 0", got)
	}
}

// TestFlatIndexScoresMatchCalculateDistance pins the public score contract
// after the ranking kernels changed: Euclid still reports the true distance
// even though it ranks by the squared value.
func TestFlatIndexScoresMatchCalculateDistance(t *testing.T) {
	vectors := [][]float32{
		{1, 2, 3, 4},
		{4, 3, 2, 1},
		{0, 0, 0, 0},
		{-1, 2, -3, 4},
		{2, 2, 2, 2},
	}
	query := []float32{1, 1, 1, 1}

	for _, metric := range []Distance{Cosine, Euclid, Dot} {
		idx := NewFlatIndex(metric)
		points := make([]PointStruct, len(vectors))
		for i, v := range vectors {
			points[i] = PointStruct{ID: "p" + string(rune('0'+i)), Vector: v}
		}
		if err := idx.Upsert(points); err != nil {
			t.Fatalf("%s: upsert: %v", metric, err)
		}

		results, err := idx.Search(query, nil, len(vectors))
		if err != nil {
			t.Fatalf("%s: search: %v", metric, err)
		}
		if len(results) != len(vectors) {
			t.Fatalf("%s: got %d results, want %d", metric, len(results), len(vectors))
		}

		byID := make(map[string]float32, len(results))
		for _, r := range results {
			byID[r.ID] = r.Score
		}
		for i, v := range vectors {
			id := "p" + string(rune('0'+i))
			want := CalculateDistance(metric, query, v)
			if math.Abs(float64(byID[id]-want)) > 1e-5 {
				t.Errorf("%s %s: score %f, want %f", metric, id, byID[id], want)
			}
		}

		// Results must be ordered best-first under the metric.
		for i := 1; i < len(results); i++ {
			if metric == Euclid {
				if results[i-1].Score > results[i].Score {
					t.Errorf("%s: results not ascending at %d: %f > %f", metric, i, results[i-1].Score, results[i].Score)
				}
			} else if results[i-1].Score < results[i].Score {
				t.Errorf("%s: results not descending at %d: %f < %f", metric, i, results[i-1].Score, results[i].Score)
			}
		}
	}
}

// TestFlatIndexMismatchedLengths keeps the documented behavior for vectors of
// different dimensions: Euclid is infinitely far, similarity metrics are 0.
func TestFlatIndexMismatchedLengths(t *testing.T) {
	for _, metric := range []Distance{Cosine, Euclid, Dot} {
		idx := NewFlatIndex(metric)
		if err := idx.Upsert([]PointStruct{{ID: "a", Vector: []float32{1, 2}}}); err != nil {
			t.Fatal(err)
		}
		results, err := idx.Search([]float32{1, 2, 3}, nil, 1)
		if err != nil {
			t.Fatal(err)
		}
		if metric == Euclid {
			if !math.IsInf(float64(results[0].Score), 1) {
				t.Errorf("Euclid: got %f, want +Inf", results[0].Score)
			}
			continue
		}
		if results[0].Score != 0 {
			t.Errorf("%s: got %f, want 0", metric, results[0].Score)
		}
	}
}
