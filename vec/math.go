package vec

import "math"

// Distance represents the metric used for vector comparison and similarity search.
// Different metrics are suitable for different use cases and vector types.
type Distance string

const (
	// Cosine measures the cosine of the angle between two vectors.
	// It is normalized by vector magnitude, making it suitable for
	// comparing vectors of different scales. Range: [-1, 1] (higher is more similar)
	Cosine Distance = "Cosine"

	// Euclid measures the straight-line distance between two vectors.
	// It is sensitive to vector magnitude. Range: [0, +inf) (lower is more similar)
	Euclid Distance = "Euclid"

	// Dot computes the dot product of two vectors.
	// It measures both magnitude and direction. Range: (-inf, +inf) (higher is more similar)
	Dot Distance = "Dot"
)

// CalculateDistance computes the similarity or distance between two vectors
// based on the specified metric. For Cosine and Dot, higher values indicate
// greater similarity. For Euclidean, lower values indicate greater similarity.
//
// Vectors of different length cannot be compared: Euclidean distance returns
// +Inf (infinitely far) and the similarity metrics return 0 (no similarity)
// instead of panicking on an out-of-range index.
func CalculateDistance(metric Distance, a, b []float32) float32 {
	if len(a) != len(b) {
		if metric == Euclid {
			return float32(math.Inf(1))
		}
		return 0
	}

	switch metric {
	case Cosine:
		return cosineSimilarity(a, b)
	case Euclid:
		return euclideanDistance(a, b)
	case Dot:
		return dotProduct(a, b)
	default:
		return cosineSimilarity(a, b)
	}
}

// dotProduct computes the dot product (inner product) of two vectors.
// It is the sum of element-wise products. Higher values indicate more similar vectors.
func dotProduct(a, b []float32) float32 {
	n := min(len(a), len(b))
	return dotF32(a[:n], b[:n])
}

// cosineSimilarity computes the cosine similarity between two vectors.
// It measures the cosine of the angle between vectors, normalized by their magnitudes.
// Returns a value between -1 (opposite) and 1 (identical), with 0 indicating orthogonality.
func cosineSimilarity(a, b []float32) float32 {
	var dot, normA, normB float32
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / float32(math.Sqrt(float64(normA))*math.Sqrt(float64(normB)))
}

// euclideanDistance computes the Euclidean (L2) distance between two vectors.
// It is the straight-line distance in vector space. Lower values indicate
// more similar vectors. Returns the actual distance (not squared).
func euclideanDistance(a, b []float32) float32 {
	var sum float32
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	// For ranking, we often skip the Sqrt for efficiency, but let's keep it standard.
	// Note: For Euclid, smaller is better. We'll handle this in sorting.
	return float32(math.Sqrt(float64(sum)))
}

// euclideanDistanceSquared is the squared Euclidean distance. Ranking by it
// is equivalent to ranking by the true distance, but avoids a square root per
// comparison; callers take the root for the final score.
func euclideanDistanceSquared(a, b []float32) float32 {
	n := min(len(a), len(b))
	return l2SquaredF32(a[:n], b[:n])
}

// cosineWithNorm is cosineSimilarity with the query's squared norm precomputed
// once, so a scan over many points does not recompute it per comparison. a and
// b must have equal length.
func cosineWithNorm(a, b []float32, normA float32) float32 {
	var dot, normB float32
	for i := range a {
		dot += a[i] * b[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / float32(math.Sqrt(float64(normA))*math.Sqrt(float64(normB)))
}

// squaredNormF32 returns the squared Euclidean norm of a, precomputed once per
// query for the cosine scan.
func squaredNormF32(a []float32) float32 {
	var sum float32
	for _, x := range a {
		sum += x * x
	}
	return sum
}
