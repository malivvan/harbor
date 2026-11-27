package vector

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// TestCollectionReload checks that Store.Collection returns the tracked live
// collection and can rebuild it from metadata for a store that has not loaded
// it yet.
func TestCollectionReload(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "vec.db")
	bdb, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	t.Cleanup(func() { _ = bdb.Close() })

	first, err := Open(ctx, bdb, Config{SharedDB: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	col, err := first.NewCollectionWithOptions(CollectionOptions{Name: "c", VectorLen: 2, Metric: Cosine})
	if err != nil {
		t.Fatalf("NewCollection: %v", err)
	}
	if err := col.Upsert([]PointStruct{{ID: "p1", Vector: []float32{1, 0}}}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Same store, already tracked: the same value comes back.
	tracked, err := first.Collection("c")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}
	if tracked != col {
		t.Fatal("Collection returned a detached copy instead of the tracked collection")
	}

	// A second store over the same handle has nothing tracked yet and must
	// rebuild the collection (and its index) from persisted metadata.
	second, err := Open(ctx, bdb, Config{SharedDB: true})
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	loaded, err := second.Collection("c")
	if err != nil {
		t.Fatalf("Collection reload: %v", err)
	}
	if loaded == col {
		t.Fatal("second store reused the first store's collection")
	}
	hits, err := loaded.Search([]float32{1, 0}, nil, 1)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 1 || hits[0].ID != "p1" {
		t.Fatalf("reloaded search = %+v", hits)
	}

	if _, err := second.Collection("missing"); !errors.Is(err, ErrCollectionNotFound) {
		t.Fatalf("Collection(missing) = %v, want ErrCollectionNotFound", err)
	}
	if _, err := second.Collection(""); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Collection(\"\") = %v, want ErrInvalidConfig", err)
	}
}
