//go:build amd64

package vec

// This file holds the amd64 distance kernels. The 4-way unrolled loops give the
// compiler explicit independent lanes to vectorize with AVX/SSE; the generic
// build (math_kernel_generic.go) is the portable fallback.

// dotF32 returns the dot product of a and b, which must have equal length.
func dotF32(a, b []float32) float32 {
	var sum float32
	i := 0
	for ; i+4 <= len(a); i += 4 {
		sum += a[i]*b[i] + a[i+1]*b[i+1] + a[i+2]*b[i+2] + a[i+3]*b[i+3]
	}
	for ; i < len(a); i++ {
		sum += a[i] * b[i]
	}
	return sum
}

// l2SquaredF32 returns the squared Euclidean distance between a and b, which
// must have equal length.
func l2SquaredF32(a, b []float32) float32 {
	var sum float32
	i := 0
	for ; i+4 <= len(a); i += 4 {
		d0 := a[i] - b[i]
		d1 := a[i+1] - b[i+1]
		d2 := a[i+2] - b[i+2]
		d3 := a[i+3] - b[i+3]
		sum += d0*d0 + d1*d1 + d2*d2 + d3*d3
	}
	for ; i < len(a); i++ {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}
