package ranking

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWeightedFusionNormalizesAndWeights(t *testing.T) {
	text := []Result{{ID: "a", Score: 10}, {ID: "b", Score: 5}}
	dense := []Result{{ID: "b", Score: 0.9}, {ID: "c", Score: 0.1}}

	got := Weighted([]float64{1, 1}, text, dense)
	require.Len(t, got, 3)
	// a = 1, b = 0 + 1, c = 0. So b and a tie at 1 and break by ID.
	require.Equal(t, "a", got[0].ID)
	require.Equal(t, "b", got[1].ID)
	require.Equal(t, "c", got[2].ID)
}

func TestWeightedFusionHonoursArmWeights(t *testing.T) {
	text := []Result{{ID: "a", Score: 1}}
	dense := []Result{{ID: "b", Score: 1}}
	got := Weighted([]float64{0.25, 1}, text, dense)
	require.Equal(t, "b", got[0].ID)
	require.Greater(t, got[0].Score, got[1].Score)
}

func TestWeightedFusionDuplicateInOneListCountsOnce(t *testing.T) {
	list := []Result{{ID: "a", Score: 2}, {ID: "a", Score: 1}, {ID: "b", Score: 1}}
	got := Weighted(nil, list)
	require.Len(t, got, 2)
	require.Equal(t, "a", got[0].ID)
}

func TestFuseZeroValueIsRRF(t *testing.T) {
	list := []Result{{ID: "a", Score: 999}}
	got := Fuse(0, 0, nil, list)
	require.Len(t, got, 1)
	require.Equal(t, 1.0/61.0, got[0].Score)
}

func TestFusionString(t *testing.T) {
	require.Equal(t, "rrf", FusionRRF.String())
	require.Equal(t, "weighted", FusionWeighted.String())
}
