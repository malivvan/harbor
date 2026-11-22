// Package vec provides a persistent vector store on top of bbolt.
//
// A [Store] owns collections. Each [Collection] stores points with a fixed
// vector dimension and a distance metric, and answers similarity searches with
// either an exact [FlatIndex] or an approximate HNSW index. Points are kept in
// memory for search and buffered on write so that many points share one bbolt
// transaction.
//
// Payloads are arbitrary key/value metadata used by [Filter] to restrict
// searches and deletes. Optional scalar quantization ([SQ8Quantizer]) shrinks
// stored vectors, and optional AEAD encryption seals points, point IDs and
// collection names.
//
// # Concurrency
//
// A Store and its Collections are safe for concurrent use. Buffered writes are
// flushed when [DefaultFlushThreshold] points accumulate, and callers that own
// the bbolt handle can force a flush with [Store.Flush].
package vec

// VectorIndex defines the standard interface for vector search engines.
// This allows transparent switching between Flat (brute-force) and HNSW (approximate) strategies.
type VectorIndex interface {
	// Upsert adds or updates points in the index.
	// If a point with the same ID already exists, it will be overwritten.
	Upsert(points []PointStruct) error

	// Search finds the nearest neighbors to the query vector, optionally applying a filter.
	// Returns up to topK results sorted by relevance.
	Search(query []float32, filter *Filter, topK int) ([]ScoredPoint, error)

	// Delete removes a point from the index by its ID.
	// Returns an error if the point does not exist.
	Delete(id string) error

	// Count returns the number of vectors currently stored in the index.
	Count() int

	// GetIDsByFilter returns all point IDs that match the given filter.
	GetIDsByFilter(filter *Filter) []string

	// GetPointsByFilter returns all points (with full payload) that match the given filter.
	GetPointsByFilter(filter *Filter) []PointStruct

	// DeleteByFilter removes all points that match the given filter and returns their IDs.
	DeleteByFilter(filter *Filter) ([]string, error)
}
