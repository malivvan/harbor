package evals

// SyntheticCorpus returns n deterministic vectors of dimension dim and a small
// set of queries derived from them.
//
// It uses an xorshift generator written here rather than math/rand so the
// bytes do not depend on the Go release or the architecture. The same seed
// therefore produces the same corpus everywhere, which is what makes a recall
// floor a regression test instead of a coin flip.
func SyntheticCorpus(n, dim int, seed int64) (vectors, queries [][]float32) {
	if n <= 0 || dim <= 0 {
		return nil, nil
	}
	state := uint64(seed)
	if state == 0 {
		state = 0x9e3779b97f4a7c15
	}
	next := func() float32 {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		return float32(state%20001)/10000 - 1
	}

	vectors = make([][]float32, n)
	for i := range vectors {
		v := make([]float32, dim)
		for j := range v {
			v[j] = next()
		}
		vectors[i] = v
	}

	qn := n / 10
	if qn < 1 {
		qn = 1
	}
	if qn > 50 {
		qn = 50
	}
	queries = make([][]float32, qn)
	for qi := range queries {
		src := vectors[(qi*7)%len(vectors)]
		q := make([]float32, dim)
		for j := range q {
			// A small perturbation keeps the query near a real point, so a
			// healthy index should recover it while an exact scan still has a
			// meaningful top-k.
			q[j] = src[j] + 0.01*next()
		}
		queries[qi] = q
	}
	return vectors, queries
}
