package ts

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergePointsSemantics(t *testing.T) {
	existing := []Point{{1, 10}, {3, 30}, {5, 50}}

	// Interleaved writes, duplicate timestamps: the last incoming value wins.
	got := mergePoints(existing, []Point{{4, 40}, {3, 33}, {6, 60}, {6, 66}})
	require.Equal(t, []Point{{1, 10}, {3, 33}, {4, 40}, {5, 50}, {6, 66}}, got)

	// Append fast path.
	got = mergePoints(existing, []Point{{7, 70}, {9, 90}})
	require.Equal(t, []Point{{1, 10}, {3, 30}, {5, 50}, {7, 70}, {9, 90}}, got)

	// Empty incoming leaves the block untouched.
	require.Equal(t, existing, mergePoints(existing, nil))

	// Empty existing block.
	require.Equal(t, []Point{{2, 2}}, mergePoints(nil, []Point{{2, 2}}))

	// The caller's input slices are not modified.
	incoming := []Point{{4, 40}, {3, 33}}
	_ = mergePoints(existing, incoming)
	require.Equal(t, []Point{{4, 40}, {3, 33}}, incoming)
}
