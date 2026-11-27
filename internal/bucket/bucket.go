// Package bucket defines the root bucket names of tempest's stores.
//
// The values are the persisted top-level bucket names: append a name for a new
// store and never rename an existing one, because on-disk data and external
// tools depend on the exact strings. They live under internal/ so the registry
// is not part of the public API; the names themselves are documented in
// README.md and AGENTS.md for tools that read a database directly.
package bucket

const (
	SYSTEM string = "system" // SYSTEM identifies the store header and key derivation.
	LEDGER string = "ledger" // LEDGER identifies the tamper-evident trace log.
	SEARCH string = "search" // SEARCH identifies the full-text store.
	RECORD string = "record" // RECORD identifies the record store.
	SERIES string = "series" // SERIES identifies the time-series store.
	VECTOR string = "vector" // VECTOR identifies the vector store.
	GRAPHS string = "graphs" // GRAPHS identifies the property-graph store.
	FSTREE string = "fstree" // FSTREE identifies the file-system store.
	OBJECT string = "object" // OBJECT identifies the flat object store.
)
