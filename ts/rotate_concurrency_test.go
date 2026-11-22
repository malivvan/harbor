package ts

import (
	"context"
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
