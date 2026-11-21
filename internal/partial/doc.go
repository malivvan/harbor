// Package partial implements a partial sort and top-k selection over generic
// slices.
//
// It is a small vendored utility: the standard library exposes sorting but no
// top-k selection, and the vector aggregation code depends on this specific
// ordering contract. It uses the standard slices package directly.
package partial
