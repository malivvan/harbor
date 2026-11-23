package series

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRangeOptions checks descending order, limit and offset against the plain
// ascending Range result, in both plaintext and block mode, and that streaming
// with options matches the materialized result.
func TestRangeOptions(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")

	plain, _ := openAEADTestStore(t, root, Config{})
	blocked, _ := openAEADTestStore(t, root, Config{
		Secret:        testKey(43),
		EncryptValues: true,
		BlockDuration: 10,
	})
	for _, store := range []*Store{plain, blocked} {
		se := store.Series("cpu")
		require.NoError(t, se.InsertMany(ctx, blockPoints()))
		require.NoError(t, se.Insert(ctx, Point{Timestamp: 9, Value: 90}))
	}

	for _, tc := range []struct {
		name  string
		store *Store
	}{
		{"plain", plain},
		{"block", blocked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			se := tc.store.Series("cpu")

			asc, err := se.Range(ctx, -100, 100)
			require.NoError(t, err)
			require.Len(t, asc, 9)

			rev := make([]Point, len(asc))
			for i := range asc {
				rev[i] = asc[len(asc)-1-i]
			}

			desc, err := se.RangeWithOptions(ctx, -100, 100, RangeOptions{Descending: true})
			require.NoError(t, err)
			require.Equal(t, rev, desc)

			lim, err := se.RangeWithOptions(ctx, -100, 100, RangeOptions{Limit: 3})
			require.NoError(t, err)
			require.Equal(t, asc[:3], lim)

			off, err := se.RangeWithOptions(ctx, -100, 100, RangeOptions{Offset: 2, Limit: 2})
			require.NoError(t, err)
			require.Equal(t, asc[2:4], off)

			descOff, err := se.RangeWithOptions(ctx, -100, 100, RangeOptions{Descending: true, Offset: 1, Limit: 2})
			require.NoError(t, err)
			require.Equal(t, rev[1:3], descOff)

			var streamed []Point
			err = se.RangeEachWithOptions(ctx, -100, 100, RangeOptions{Descending: true, Limit: 4}, func(p Point) error {
				streamed = append(streamed, p)
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, rev[:4], streamed)

			_, err = se.RangeWithOptions(ctx, -100, 100, RangeOptions{Limit: -1})
			require.ErrorIs(t, err, ErrInvalidOptions)
			err = se.RangeEachWithOptions(ctx, -100, 100, RangeOptions{Offset: -1}, func(Point) error { return nil })
			require.ErrorIs(t, err, ErrInvalidOptions)
		})
	}
}
