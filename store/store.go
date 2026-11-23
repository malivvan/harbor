// Package store defines the stable numeric identifiers for Harbor's stores.
//
// The values are part of the public registry: append a new identifier for a new
// store and never renumber an existing one. Persisted or externally observed
// code may depend on the exact number.
package store

const (
	// Audit identifies the tamper-evident audit trail.
	Audit int8 = -2
	// SEARCH identifies the full-text store.
	SEARCH int8 = -1
	// SYSTEM identifies the store header and key derivation.
	SYSTEM int8 = 0
	// RECORD identifies the record store.
	RECORD int8 = 1
	// SERIES identifies the time-series store.
	SERIES int8 = 2
	// VECTOR identifies the vector store.
	VECTOR int8 = 3
	// GRAPHS identifies the property-graph store.
	GRAPHS int8 = 4
	// FSTREE identifies the file-system store.
	FSTREE int8 = 5
	// OBJECT identifies the flat object store.
	OBJECT int8 = 6
)
