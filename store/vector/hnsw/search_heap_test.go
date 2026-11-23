package hnsw

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCandidateSetMatchesSortedSlice interleaves min and worst pops and checks
// both ends against an independently sorted slice of the active entries.
func TestCandidateSetMatchesSortedSlice(t *testing.T) {
	var set candidateSet[int]
	var entries []*searchEntry[int]

	for i := 0; i < 200; i++ {
		node := &layerNode[int]{Node: Node[int]{Key: i}}
		set.Push(node, float32((i*37)%101))
		entries = append(entries, &searchEntry[int]{node: node, dist: float32((i * 37) % 101), alive: true})
	}

	active := func() []*searchEntry[int] {
		var out []*searchEntry[int]
		for _, e := range entries {
			if e.alive {
				out = append(out, e)
			}
		}
		sort.Slice(out, func(i, j int) bool { return out[i].dist < out[j].dist })
		return out
	}

	for step := 0; step < 150; step++ {
		require.Equal(t, len(active()), set.Len())

		if step%2 == 0 {
			want := active()[0]
			got := set.PopMin()
			require.Equal(t, want.dist, got.dist)
			// Mark the corresponding original entry dead.
			for _, e := range entries {
				if e.alive && e.node == got.node {
					e.alive = false
					break
				}
			}
		} else {
			sorted := active()
			want := sorted[len(sorted)-1]
			got := set.PopWorst()
			require.Equal(t, want.dist, got.dist)
			for _, e := range entries {
				if e.alive && e.node == got.node {
					e.alive = false
					break
				}
			}
		}
	}

	// Peeking and popping the remainder drains in ascending order.
	last := float32(-1)
	for set.Len() > 0 {
		e := set.PopMin()
		require.GreaterOrEqual(t, e.dist, last)
		last = e.dist
	}
}
