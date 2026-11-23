package fstree

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func fsConcurrencyKey(seed byte) []byte {
	key := make([]byte, KeySize)
	for i := range key {
		key[i] = seed + byte(i)
	}
	return key
}

// TestConcurrentWriteAndRotate is a regression test for rotation vs file I/O.
func TestConcurrentWriteAndRotate(t *testing.T) {
	bdb, err := bolt.Open(t.TempDir()+"/concurrent.db", 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	ctx := context.Background()
	store, err := Open(ctx, bdb, Config{Secret: fsConcurrencyKey(1), EncryptContent: true})
	require.NoError(t, err)
	f, err := store.Open("")
	require.NoError(t, err)
	writeStoreFile(t, f, "/seed.txt", "seed")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			_ = f.Truncate("/worker.txt", 0)
			fh, err := f.OpenFile("/worker.txt", 0x241, 0644) // O_CREATE|O_RDWR|O_TRUNC
			if err == nil {
				_, _ = fh.Write([]byte("worker"))
				_ = fh.Close()
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			require.NoError(t, store.RotateAll(ctx, fsConcurrencyKey(1)))
		}
	}()
	wg.Wait()

	require.NoError(t, store.VerifyAll(ctx))
	data, err := f.ReadFile("/seed.txt")
	require.NoError(t, err)
	require.Equal(t, "seed", string(data))
}
