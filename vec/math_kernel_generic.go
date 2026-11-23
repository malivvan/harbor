//go:build !amd64

package vec

// Portable distance kernels for architectures without a tuned build. They
// match the amd64 kernels in math_kernel_amd64.go exactly.

// dotF32 returns the dot product of a and b, which must have equal length.
func dotF32(a, b []float32) float32 {
	var sum float32
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}

// l2SquaredF32 returns the squared Euclidean distance between a and b, which
// must have equal length.
func l2SquaredF32(a, b []float32) float32 {
	var sum float32
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}
