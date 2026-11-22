package ts

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func tsConcurrencyKey(seed byte) []byte {
	key := make([]byte, KeySize)
	for i := range key {
		key[i] = seed + byte(i)
	}
	return key
}

// TestConcurrentInsertAndRotate is a regression test for rotation vs inserts.
func TestConcurrentInsertAndRotate(t *testing.T) {
	bdb, err := bolt.Open(t.TempDir()+"/concurrent.db", 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	ctx := context.Background()
	store, err := Open(ctx, bdb, Config{Secret: tsConcurrencyKey(1), EncryptValues: true, Root: [][]byte{[]byte("ts")}})
	require.NoError(t, err)
	series := store.Series("cpu")
	require.NoError(t, series.Insert(ctx, Point{Timestamp: 100, Value: 1}))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			_ = series.Insert(ctx, Point{Timestamp: int64(i), Value: float64(i)})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			require.NoError(t, store.RotateAll(ctx, tsConcurrencyKey(1)))
		}
	}()
	wg.Wait()

	require.NoError(t, store.VerifyAll(ctx))
	points, err := series.Range(ctx, 0, 200)
	require.NoError(t, err)
	require.NotEmpty(t, points)
}

// TestRotateAllSeesConcurrentlyCreatedSeries regresses a rotation race: the
// series list was collected in a separate read transaction, so a series
// created between that snapshot and the rewrite stayed sealed under the old
// key after the new key dropped it, making the series undecryptable.
func TestRotateAllSeesConcurrentlyCreatedSeries(t *testing.T) {
	bdb, err := bolt.Open(t.TempDir()+"/rotate-new-series.db", 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	ctx := context.Background()
	store, err := Open(ctx, bdb, Config{Secret: tsConcurrencyKey(1), EncryptValues: true, Root: [][]byte{[]byte("ts")}})
	require.NoError(t, err)
	require.NoError(t, store.Series("base").Insert(ctx, Point{Timestamp: 1, Value: 1}))

	const writers = 4
	const perWriter = 40
	start := make(chan struct{})
	errCh := make(chan error, writers*perWriter)
	var wg sync.WaitGroup
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < perWriter; i++ {
				name := fmt.Sprintf("s-%d-%d", w, i)
				if err := store.Series(name).Insert(ctx, Point{Timestamp: int64(i + 1), Value: float64(i)}); err != nil {
					errCh <- err
				}
			}
		}(w)
	}

	close(start)
	for i := 0; i < 5; i++ {
		require.NoError(t, store.RotateAll(ctx, tsConcurrencyKey(byte(2+i))))
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	require.NoError(t, store.VerifyAll(ctx))
}
