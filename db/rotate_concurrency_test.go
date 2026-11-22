package db

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

type concurrentRec struct {
	ID   int    `db:"id,increment"`
	Name string `db:"index"`
}

func testKey(seed byte) []byte {
	key := make([]byte, KeySize)
	for i := range key {
		key[i] = seed + byte(i)
	}
	return key
}

// TestConcurrentSaveAndReEncrypt is a regression test for the rotation/encState
// data race: it must pass under -race.
func TestConcurrentSaveAndReEncrypt(t *testing.T) {
	bdb, err := bolt.Open(t.TempDir()+"/concurrent.db", 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	ctx := context.Background()
	store, err := Open(ctx, bdb, Config{Secret: testKey(1), EncryptValues: true, EncryptKeys: true})
	require.NoError(t, err)
	require.NoError(t, store.Save(ctx, &concurrentRec{Name: "seed"}))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			_ = store.Save(ctx, &concurrentRec{Name: "worker"})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			require.NoError(t, store.ReEncrypt(ctx, testKey(1)))
		}
	}()
	wg.Wait()

	require.NoError(t, store.VerifyAll(ctx))
	var all []concurrentRec
	require.NoError(t, store.All(ctx, &all))
	require.NotEmpty(t, all)
}
