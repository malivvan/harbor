package db

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

type aggSale struct {
	ID     int    `db:"id,increment"`
	Kind   string `db:"index"`
	Lang   string `db:"index"`
	Amount float64
	At     time.Time
}

type aggOptional struct {
	ID     int     `db:"id,increment"`
	Amount float64 `json:"Amount,omitempty"`
}

type aggMixed struct {
	ID    int `db:"id,increment"`
	Value any
}

func newAggStore(t *testing.T) *Store {
	t.Helper()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "agg.db"), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	st, err := Open(context.Background(), bdb, Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close(); _ = bdb.Close() })
	return st
}

func seedAgg(t *testing.T, st *Store) {
	t.Helper()
	ctx := context.Background()
	day := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	sales := []aggSale{
		{Kind: "a", Lang: "en", Amount: 10, At: day.Add(1 * time.Hour)},
		{Kind: "a", Lang: "en", Amount: 20, At: day.Add(2 * time.Hour)},
		{Kind: "a", Lang: "fr", Amount: 30, At: day.AddDate(0, 0, 1).Add(1 * time.Hour)},
		{Kind: "b", Lang: "en", Amount: 5, At: day.AddDate(0, 0, 1).Add(2 * time.Hour)},
		{Kind: "b", Lang: "fr", Amount: 15, At: day.AddDate(0, 0, 2)},
	}
	for i := range sales {
		require.NoError(t, st.Save(ctx, &sales[i]))
	}
}

func groupByKind(t *testing.T, result AggregateResult) map[string]Group {
	t.Helper()
	out := make(map[string]Group, len(result.Groups))
	for _, g := range result.Groups {
		kind, _ := g.Keys["Kind"].(string)
		out[kind] = g
	}
	return out
}

func TestAggregateBasicMeasures(t *testing.T) {
	ctx := context.Background()
	st := newAggStore(t)
	seedAgg(t, st)

	result, err := st.From("aggSale").Aggregate(ctx, AggregateSpec{
		GroupBy: []string{"Kind"},
		Measures: []Measure{
			{Alias: "rows", Type: MeasureCount},
			{Alias: "sum", Type: MeasureSum, Field: "Amount"},
			{Alias: "avg", Type: MeasureAvg, Field: "Amount"},
			{Alias: "min", Type: MeasureMin, Field: "Amount"},
			{Alias: "max", Type: MeasureMax, Field: "Amount"},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Groups, 2)

	a := groupByKind(t, result)["a"]
	require.Equal(t, int64(3), a.Count)
	require.Equal(t, 3.0, a.Measures["rows"])
	require.Equal(t, 60.0, a.Measures["sum"])
	require.Equal(t, 20.0, a.Measures["avg"])
	require.Equal(t, 10.0, a.Measures["min"])
	require.Equal(t, 30.0, a.Measures["max"])

	b := groupByKind(t, result)["b"]
	require.Equal(t, 2.0, b.Measures["rows"])
	require.Equal(t, 20.0, b.Measures["sum"])
}

func TestAggregateMultipleGroupKeysAndOrdering(t *testing.T) {
	ctx := context.Background()
	st := newAggStore(t)
	seedAgg(t, st)

	result, err := st.From("aggSale").Aggregate(ctx, AggregateSpec{
		GroupBy: []string{"Kind", "Lang"},
		Measures: []Measure{
			{Alias: "sum", Type: MeasureSum, Field: "Amount"},
		},
		OrderBy: "sum",
		Desc:    true,
		Limit:   3,
	})
	require.NoError(t, err)
	require.Len(t, result.Groups, 3)
	// a/en and a/fr both hold 30; ties order by group key.
	require.Equal(t, 30.0, result.Groups[0].Measures["sum"])
	require.Equal(t, "a", result.Groups[0].Keys["Kind"])
	require.Equal(t, "en", result.Groups[0].Keys["Lang"])
	require.Equal(t, 30.0, result.Groups[1].Measures["sum"])
	require.Equal(t, 15.0, result.Groups[2].Measures["sum"])
}

func TestAggregateHaving(t *testing.T) {
	ctx := context.Background()
	st := newAggStore(t)
	seedAgg(t, st)

	result, err := st.From("aggSale").Aggregate(ctx, AggregateSpec{
		GroupBy:  []string{"Kind"},
		Measures: []Measure{{Alias: "total", Type: MeasureSum, Field: "Amount"}},
		Having:   []Having{{Alias: "total", Op: HavingGTE, Value: 25}},
		OrderBy:  "total",
	})
	require.NoError(t, err)
	require.Len(t, result.Groups, 1)
	require.Equal(t, "a", result.Groups[0].Keys["Kind"])

	// Having can reference the group's record count.
	result, err = st.From("aggSale").Aggregate(ctx, AggregateSpec{
		GroupBy: []string{"Kind"},
		Having:  []Having{{Alias: "count", Op: HavingEQ, Value: 2}},
	})
	require.NoError(t, err)
	require.Len(t, result.Groups, 1)
	require.Equal(t, "b", result.Groups[0].Keys["Kind"])
}

func TestAggregateMedianPercentileStdDev(t *testing.T) {
	ctx := context.Background()
	st := newAggStore(t)
	seedAgg(t, st)

	result, err := st.From("aggSale").Aggregate(ctx, AggregateSpec{
		GroupBy: []string{"Kind"},
		Measures: []Measure{
			{Alias: "median", Type: MeasureMedian, Field: "Amount"},
			{Alias: "p90", Type: MeasurePercentile, Field: "Amount", P: 90},
			{Alias: "sd", Type: MeasureStdDev, Field: "Amount"},
			{Alias: "sdp", Type: MeasureStdDevPop, Field: "Amount"},
		},
	})
	require.NoError(t, err)

	a := groupByKind(t, result)["a"]
	require.Equal(t, 20.0, a.Measures["median"])
	require.InDelta(t, 28.0, a.Measures["p90"], 1e-9)
	require.InDelta(t, 10.0, a.Measures["sd"], 1e-9)
	require.InDelta(t, 8.16496580927726, a.Measures["sdp"], 1e-6)

	// Group b holds 5 and 15: sample sd = sqrt(50), population sd = 5.
	b := groupByKind(t, result)["b"]
	require.InDelta(t, 7.0710678118654755, b.Measures["sd"], 1e-9)
	require.InDelta(t, 5.0, b.Measures["sdp"], 1e-9)
}

func TestAggregateTimeBuckets(t *testing.T) {
	ctx := context.Background()
	st := newAggStore(t)
	seedAgg(t, st)

	result, err := st.From("aggSale").Aggregate(ctx, AggregateSpec{
		Measures:   []Measure{{Alias: "rows", Type: MeasureCount}},
		TimeBucket: &TimeBucket{Field: "At", Unit: TimeUnitDay},
		OrderBy:    "At",
	})
	require.NoError(t, err)
	require.Len(t, result.Groups, 3)
	first, ok := result.Groups[0].Keys["At"].(time.Time)
	require.True(t, ok)
	require.Equal(t, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), first)
	require.Equal(t, 2.0, result.Groups[0].Measures["rows"])
}

func TestAggregateMissingAndNonNumericPolicies(t *testing.T) {
	ctx := context.Background()

	t.Run("missing excluded by default", func(t *testing.T) {
		st := newAggStore(t)
		require.NoError(t, st.Save(ctx, &aggOptional{Amount: 10}))
		require.NoError(t, st.Save(ctx, &aggOptional{Amount: 0}))

		result, err := st.From("aggOptional").Aggregate(ctx, AggregateSpec{
			Measures: []Measure{{Alias: "sum", Type: MeasureSum, Field: "Amount"}},
		})
		require.NoError(t, err)
		require.Equal(t, 10.0, result.Groups[0].Measures["sum"])
		require.Equal(t, int64(1), result.Skipped)
	})

	t.Run("missing as zero", func(t *testing.T) {
		st := newAggStore(t)
		require.NoError(t, st.Save(ctx, &aggOptional{Amount: 10}))
		require.NoError(t, st.Save(ctx, &aggOptional{Amount: 0}))

		result, err := st.From("aggOptional").Aggregate(ctx, AggregateSpec{
			Measures: []Measure{{Alias: "sum", Type: MeasureSum, Field: "Amount"}},
			Missing:  MissingZero,
		})
		require.NoError(t, err)
		require.Equal(t, 10.0, result.Groups[0].Measures["sum"])
		require.Zero(t, result.Skipped)
	})

	t.Run("non-numeric policies", func(t *testing.T) {
		st := newAggStore(t)
		require.NoError(t, st.Save(ctx, &aggMixed{Value: 3.0}))
		require.NoError(t, st.Save(ctx, &aggMixed{Value: "nope"}))

		_, err := st.From("aggMixed").Aggregate(ctx, AggregateSpec{
			Measures: []Measure{{Alias: "sum", Type: MeasureSum, Field: "Value"}},
		})
		require.ErrorIs(t, err, ErrInvalidAggregate)

		result, err := st.From("aggMixed").Aggregate(ctx, AggregateSpec{
			Measures:   []Measure{{Alias: "sum", Type: MeasureSum, Field: "Value"}},
			NonNumeric: NonNumericSkip,
		})
		require.NoError(t, err)
		require.Equal(t, 3.0, result.Groups[0].Measures["sum"])
		require.Equal(t, int64(1), result.Skipped)

		result, err = st.From("aggMixed").Aggregate(ctx, AggregateSpec{
			Measures:   []Measure{{Alias: "sum", Type: MeasureSum, Field: "Value"}},
			NonNumeric: NonNumericZero,
		})
		require.NoError(t, err)
		require.Equal(t, 3.0, result.Groups[0].Measures["sum"])
	})
}

func TestAggregateEmptyBucket(t *testing.T) {
	ctx := context.Background()
	st := newAggStore(t)

	result, err := st.From("aggSale").Aggregate(ctx, AggregateSpec{
		Measures: []Measure{{Alias: "rows", Type: MeasureCount}},
	})
	require.NoError(t, err)
	require.Empty(t, result.Groups)
}

func TestAggregateValidation(t *testing.T) {
	ctx := context.Background()
	st := newAggStore(t)

	bad := []AggregateSpec{
		{Limit: -1},
		{GroupBy: []string{""}},
		{Measures: []Measure{{Alias: "x", Type: MeasureSum}}},
		{Measures: []Measure{{Alias: "x", Type: "nope", Field: "Amount"}}},
		{Measures: []Measure{{Alias: "x", Type: MeasurePercentile, Field: "Amount", P: 101}}},
		{Measures: []Measure{{Alias: "x", Type: MeasureSum, Field: "Amount"}, {Alias: "x", Type: MeasureMax, Field: "Amount"}}},
		{OrderBy: "missing"},
		{Having: []Having{{Alias: "missing", Op: HavingGT, Value: 1}}},
		{Having: []Having{{Alias: "count", Op: "nope", Value: 1}}},
		{TimeBucket: &TimeBucket{Field: "At", Unit: "decade"}},
		{TimeBucket: &TimeBucket{Unit: TimeUnitDay}},
	}
	for i, spec := range bad {
		if _, err := st.From("aggSale").Aggregate(ctx, spec); !errors.Is(err, ErrInvalidAggregate) {
			t.Errorf("spec %d: err = %v, want ErrInvalidAggregate", i, err)
		}
	}
}

func TestAggregateCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	st := newAggStore(t)
	_, err := st.From("aggSale").Aggregate(ctx, AggregateSpec{
		Measures: []Measure{{Alias: "rows", Type: MeasureCount}},
	})
	require.ErrorIs(t, err, context.Canceled)
}
