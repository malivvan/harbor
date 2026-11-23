// Package hnsw implements a Hierarchical Navigable Small World graph for
// approximate nearest-neighbour search.
//
// The graph stores float32 vectors of one fixed dimension. [Graph.Search]
// returns the k closest nodes, and [SavedGraph] adds file persistence on top
// of [Graph.Export] and [Graph.Import]. Distance functions are pluggable:
// [CosineDistance] and [EuclideanDistance] are built in, and
// [RegisterDistanceFunc] makes a custom function available to the export
// format.
//
// The package is derived from github.com/coder/hnsw and keeps that project's
// graph semantics. It is vendored here so that the vector store depends only on
// this module.
package hnsw
