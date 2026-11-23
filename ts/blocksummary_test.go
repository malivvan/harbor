package ts

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// TestBlockSummaryAggregatesMatchScan verifies the block-summary fast path
// produces exactly the same samples as an exact point scan for every aggregate
// function, across window sizes that both align with and cross block
// boundaries.
func TestBlockSummaryAggregatesMatchScan(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")

	plain, _ := openAEADTestStore(t, root, Config{})
	blocked, _ := openAEADTestStore(t, root, Config{
		Secret:        testKey(41),
		EncryptValues: true,
		BlockDuration: 10,
	})

	for _, store := range []*Store{plain, blocked} {
		se := store.Series("cpu")
		require.NoError(t, se.InsertMany(ctx, blockPoints()))
		require.NoError(t, se.Insert(ctx, Point{Timestamp: 9, Value: 90}))
	}

	plainSeries := plain.Series("cpu")
	blockedSeries := blocked.Series("cpu")

	fns := []AggregateFunc{AggMin, AggMax, AggAvg, AggSum, AggCount, AggFirst, AggLast}
	steps := []int64{1, 3, 5, 10, 25, 100}
	windows := [][2]int64{{-10, 30}, {0, 25}, {0, 9}, {9, 20}, {10, 20}}

	for _, fn := range fns {
		for _, step := range steps {
			for _, w := range windows {
				want, err := plainSeries.Aggregate(ctx, w[0], w[1], step, fn)
				require.NoError(t, err)
				got, err := blockedSeries.Aggregate(ctx, w[0], w[1], step, fn)
				require.NoError(t, err)
				require.Equalf(t, want, got, "fn=%s step=%d window=%v", fn, step, w)
			}
		}
	}
}

// TestBlockSummaryPersisted verifies the stored block value carries a summary
// that matches the points it holds.
func TestBlockSummaryPersisted(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")
	store, db := openAEADTestStore(t, root, Config{
		Secret:        testKey(42),
		EncryptValues: true,
		BlockDuration: 10,
	})
	require.NoError(t, store.Series("cpu").InsertMany(ctx, blockPoints()))

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(root).Bucket([]byte("cpu"))
		require.NotNil(t, bucket)
		key := encodeKey(0)
		raw := bucket.Get(key[:])
		require.NotNil(t, raw)

		sum, ok, err := store.decodeBlockSummaryValue(raw)
		require.NoError(t, err)
		require.True(t, ok)

		points, err := store.openBlock(raw)
		require.NoError(t, err)
		require.Equal(t, summarize(points), sum)
		return nil
	}))
}
