// Package store defines the root bucket names of Harbor's stores.
//
// The values are part of the public registry: append a new name for a new
// store and never rename an existing one. They are the persisted top-level
// bucket names, so on-disk data and external tools depend on the exact
// strings.
package store

const (
	SYSTEM string = "system" // SYSTEM identifies the store header and key derivation.
	LEDGER string = "ledger" // LEDGER identifies the tamper-evident audit trail.
	SEARCH string = "search" // SEARCH identifies the full-text store.
	RECORD string = "record" // RECORD identifies the record store.
	SERIES string = "series" // SERIES identifies the time-series store.
	VECTOR string = "vector" // VECTOR identifies the vector store.
	GRAPHS string = "graphs" // GRAPHS identifies the property-graph store.
	FSTREE string = "fstree" // FSTREE identifies the file-system store.
	OBJECT string = "object" // OBJECT identifies the flat object store.
)
