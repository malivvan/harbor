package vector

import (
	"context"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// openTestStorage opens a bbolt database and wraps it in a Store for tests.
// The Store closes the bbolt handle, so cleanup only needs to close the Store.
func openTestStorage(t *testing.T, path string) *Store {
	t.Helper()
	return openTestStorageOptions(t, path, nil, Config{})
}

func openTestStorageOptions(t *testing.T, path string, options *bolt.Options, cfg Config) *Store {
	t.Helper()

	if options == nil {
		options = &bolt.Options{Timeout: time.Second}
	}
	bdb, err := bolt.Open(path, 0600, options)
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	store, err := Open(context.Background(), bdb, cfg)
	if err != nil {
		_ = bdb.Close()
		t.Fatalf("vec.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}
