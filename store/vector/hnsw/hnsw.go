package hnsw

// mapsKeys returns the keys of the map m.
// The keys will be in an indeterminate order.
//
// The simplest true equivalent using the standard library  is:
//
//	slices.AppendSeq(make([]K, 0, len(m)), maps.Keys(m))
func mapsKeys[M ~map[K]V, K comparable, V any](m M) []K {

	r := make([]K, 0, len(m))
	for k := range m {
		r = append(r, k)
	}
	return r
}
