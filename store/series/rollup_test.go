package series

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func rollupPoints(t *testing.T, store *Store, name string, points []Point) {
	t.Helper()
	for _, point := range points {
		require.NoError(t, store.Series(name).Insert(context.Background(), point))
	}
}

func TestRollupOnInsertSum(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	require.NoError(t, store.CreateRollup(ctx, RollupSpec{
		Source: "raw", Target: "hourly", Step: 3600, Func: AggSum, OnInsert: true,
	}))
	rollupPoints(t, store, "raw", []Point{
		{Timestamp: 0, Value: 1},
		{Timestamp: 1800, Value: 2},
		{Timestamp: 3600, Value: 3},
		{Timestamp: 5400, Value: 4},
	})
	out, err := store.Series("hourly").Range(ctx, 0, 7200)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Equal(t, int64(0), out[0].Timestamp)
	require.Equal(t, float64(3), out[0].Value)
	require.Equal(t, int64(3600), out[1].Timestamp)
	require.Equal(t, float64(7), out[1].Value)
}

func TestRollupAggregates(t *testing.T) {
	cases := []struct {
		fn   AggregateFunc
		want float64
	}{
		{AggMin, 1}, {AggMax, 4}, {AggAvg, 2.5}, {AggSum, 10}, {AggCount, 4}, {AggFirst, 1}, {AggLast, 4},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(string(tc.fn), func(t *testing.T) {
			store, _ := openTestStore(t, nil)
			ctx := context.Background()
			require.NoError(t, store.CreateRollup(ctx, RollupSpec{
				Source: "raw", Target: "rollup", Step: 3600, Func: tc.fn, OnInsert: true,
			}))
			rollupPoints(t, store, "raw", []Point{
				{Timestamp: 0, Value: 1},
				{Timestamp: 900, Value: 2},
				{Timestamp: 1800, Value: 3},
				{Timestamp: 2700, Value: 4},
			})
			point, err := store.Series("rollup").Get(ctx, 0)
			require.NoError(t, err)
			require.Equal(t, tc.want, point.Value)
		})
	}
}

func TestRunRollupsBackfill(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	rollupPoints(t, store, "raw", []Point{
		{Timestamp: 0, Value: 1},
		{Timestamp: 1800, Value: 2},
		{Timestamp: 3600, Value: 3},
	})
	require.NoError(t, store.CreateRollup(ctx, RollupSpec{
		Source: "raw", Target: "hourly", Step: 3600, Func: AggSum,
	}))
	n, err := store.RunRollups(ctx, "raw", 0, 7200)
	require.NoError(t, err)
	require.Equal(t, 2, n) // windows 0 and 3600; 7200 has no source points

	out, err := store.Series("hourly").Range(ctx, 0, 7200)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Equal(t, float64(3), out[0].Value)
	require.Equal(t, float64(3), out[1].Value)
}

func TestDropRollupOwnership(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	require.NoError(t, store.CreateRollup(ctx, RollupSpec{
		Source: "raw", Target: "hourly", Step: 3600, Func: AggSum, OnInsert: true,
	}))
	rollupPoints(t, store, "raw", []Point{{Timestamp: 0, Value: 1}})
	require.NoError(t, store.DropRollup(ctx, "hourly"))
	out, _ := store.Series("hourly").Range(ctx, 0, 3600)
	require.Empty(t, out)

	require.NoError(t, store.CreateRollup(ctx, RollupSpec{
		Source: "raw", Target: "shared", Step: 3600, Func: AggSum, OnInsert: true, SharedTarget: true,
	}))
	rollupPoints(t, store, "raw", []Point{{Timestamp: 0, Value: 1}})
	require.NoError(t, store.DropRollup(ctx, "shared"))
	out, err := store.Series("shared").Range(ctx, 0, 3600)
	require.NoError(t, err)
	require.Len(t, out, 1)
}

func TestSeriesMatching(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	require.NoError(t, store.Series("cpu").Insert(ctx, Point{Timestamp: 1, Value: 1}))
	require.NoError(t, store.Series("mem").Insert(ctx, Point{Timestamp: 1, Value: 1}))
	require.NoError(t, store.Series("cpu").SetLabels(ctx, map[string]string{"host": "a", "kind": "cpu"}))
	require.NoError(t, store.Series("mem").SetLabels(ctx, map[string]string{"host": "b", "kind": "mem"}))

	infos, err := store.SeriesMatching(ctx, LabelMatcher{Key: "host", Value: "a"})
	require.NoError(t, err)
	require.Len(t, infos, 1)
	require.Equal(t, "cpu", infos[0].Name)

	infos, err = store.SeriesMatching(ctx, LabelMatcher{Key: "kind"})
	require.NoError(t, err)
	require.Len(t, infos, 2)

	infos, err = store.SeriesMatching(ctx, LabelMatcher{Key: "missing", Value: "x"})
	require.NoError(t, err)
	require.Empty(t, infos)
}

func TestRollupSpecValidation(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	require.ErrorIs(t, store.CreateRollup(ctx, RollupSpec{}), ErrInvalidRollup)
	require.ErrorIs(t, store.CreateRollup(ctx, RollupSpec{Source: "a", Target: "a", Step: 1, Func: AggSum}), ErrInvalidRollup)
	require.ErrorIs(t, store.CreateRollup(ctx, RollupSpec{Source: "a", Target: "b", Step: 0, Func: AggSum}), ErrInvalidRollup)
	require.ErrorIs(t, store.CreateRollup(ctx, RollupSpec{Source: "a", Target: "b", Step: 1, Func: "bogus"}), ErrInvalidRollup)
}
