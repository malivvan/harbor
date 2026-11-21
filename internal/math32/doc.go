// Package math32 is a float32 port of the standard library's math package.
//
// The Go standard library has no float32 math package, and vek32 relies on
// these functions for its 32-bit kernels. It is intentionally kept in-tree;
// the port tracks the upstream math package and is covered by its own tests.
package math32
