package series

import (
	"context"
	"encoding/binary"
	"math"
	"sync"
	"testing"
	"time"

	hstore "github.com/malivvan/tempest/store"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// TestPruneBucketDeletesOnlyOldestKeys covers the stage-then-apply rewrite of
// pruneBucket: deleting keys no longer mutates the bucket mid-traversal, and
// exactly the oldest keys are removed.
func TestPruneBucketDeletesOnlyOldestKeys(t *testing.T) {
	_, db := openTestStore(t, nil)

	const total = 100
	const keep = 10

	removed := -1
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("prune"))
		if err != nil {
			return err
		}
		for i := 0; i < total; i++ {
			key := encodeKey(int64(i))
			if err := b.Put(key[:], []byte{byte(i)}); err != nil {
				return err
			}
		}
		var pruneErr error
		removed, pruneErr = pruneBucket(context.Background(), b, Retention{MaxPoints: keep}, 0)
		return pruneErr
	})
	require.NoError(t, err)
	require.Equal(t, total-keep, removed)

	var remaining []int64
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("prune"))
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			remaining = append(remaining, decodeKey(k))
		}
		return nil
	})
	require.NoError(t, err)

	want := make([]int64, 0, keep)
	for i := total - keep; i < total; i++ {
		want = append(want, int64(i))
	}
	require.Equal(t, want, remaining, "only the newest keys must survive")
}

// TestSetNowConcurrent exercises SetNow racing against nowNanos reads (via
// inserts and direct calls). Run with -race; the atomic clock keeps it clean.
func TestSetNowConcurrent(t *testing.T) {
	store, _ := openTestStore(t, nil)
	ctx := context.Background()
	se := store.Series("race")

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(base int64) {
			defer wg.Done()
			for j := 0; ; j++ {
				select {
				case <-stop:
					return
				default:
				}
				store.SetNow(func() int64 { return base + int64(j) })
			}
		}(int64(i) * 1_000_000)
	}

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(off int64) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = store.nowNanos()
				_ = se.Insert(ctx, Point{Timestamp: off + int64(j), Value: float64(j)})
			}
		}(int64(i) * 10_000)
	}

	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestUnpackBlockRejectsOverflowingCount covers the uint32 count guard that
// replaced the dead "count < 0" check.
func TestUnpackBlockRejectsOverflowingCount(t *testing.T) {
	header := make([]byte, blockHeaderSize)
	binary.LittleEndian.PutUint32(header, math.MaxUint32)
	_, err := unpackBlock(header)
	require.ErrorIs(t, err, errCorruptPoint)

	// A count that overflows a 32-bit int when multiplied by 16 must also be
	// rejected rather than triggering a bogus size match.
	header2 := make([]byte, blockHeaderSize)
	binary.LittleEndian.PutUint32(header2, math.MaxUint32/16+1)
	_, err = unpackBlock(header2)
	require.ErrorIs(t, err, errCorruptPoint)
}

// TestAggregateBlockModeStreamsAcrossBlocks ensures the streaming block-mode
// aggregation covers points spread across many sealed blocks.
func TestAggregateBlockModeStreamsAcrossBlocks(t *testing.T) {
	ctx := context.Background()
	store := openBlockStore(t, []byte(hstore.SERIES), 30, 10)
	se := store.Series("cpu")

	const total = 500
	points := make([]Point, 0, total)
	for i := 0; i < total; i++ {
		points = append(points, Point{Timestamp: int64(i), Value: float64(i % 7)})
	}
	require.NoError(t, se.InsertMany(ctx, points))

	const step = 50
	samples, err := se.Aggregate(ctx, 0, total-1, step, AggSum)
	require.NoError(t, err)

	var want []Sample
	for w := 0; w < total; w += step {
		sum := 0.0
		for i := w; i < w+step && i < total; i++ {
			sum += float64(i % 7)
		}
		want = append(want, Sample{Timestamp: int64(w), Value: sum})
	}
	require.Equal(t, want, samples)

	// Count aggregation must see every point, not a truncated window.
	count, err := se.Aggregate(ctx, 0, total-1, step, AggCount)
	require.NoError(t, err)
	require.NotEmpty(t, count)
	for _, s := range count {
		require.Equal(t, float64(step), s.Value)
	}
}
