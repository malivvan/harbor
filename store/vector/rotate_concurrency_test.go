package vector

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func concurrencyKey(seed byte) []byte {
	key := make([]byte, KeySize)
	for i := range key {
		key[i] = seed + byte(i)
	}
	return key
}

// TestConcurrentFlushAndRotate is a regression test for the vec rotation data
// race; run with -race.
func TestConcurrentFlushAndRotate(t *testing.T) {
	bdb, err := bolt.Open(t.TempDir()+"/concurrent.db", 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	ctx := context.Background()
	store, err := Open(ctx, bdb, Config{Secret: concurrencyKey(1), EncryptPoints: true})
	require.NoError(t, err)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]PointStruct{{ID: "seed", Vector: []float32{1, 0}}}))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			_ = col.Upsert([]PointStruct{{ID: "worker", Vector: []float32{1, 0}}})
			_ = col.Flush()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			require.NoError(t, store.RotateAll(ctx, concurrencyKey(1)))
		}
	}()
	wg.Wait()

	require.NoError(t, store.VerifyAll(ctx))
	loaded, err := store.LoadCollection("docs")
	require.NoError(t, err)
	require.NotEmpty(t, loaded)
}
