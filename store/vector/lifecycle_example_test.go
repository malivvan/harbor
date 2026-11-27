package vector_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/malivvan/eidos-db/store/vector"
	bolt "go.etcd.io/bbolt"
)

// ExampleStore_sharedHandle shows the embedding lifecycle: the caller owns the
// bbolt handle, Close only flushes and releases the wrapper, and a collection
// reports both buffered work and its closed state.
func ExampleStore_sharedHandle() {
	dir, err := os.MkdirTemp("", "eidos-vec-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	bdb, err := bolt.Open(filepath.Join(dir, "vectors.db"), 0o600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer bdb.Close()

	// SharedDB: the caller keeps ownership of bdb.
	store, err := vector.Open(context.Background(), bdb, vector.Config{SharedDB: true})
	if err != nil {
		log.Fatal(err)
	}
	docs, err := store.NewCollection("docs", 2, vector.Cosine, false)
	if err != nil {
		log.Fatal(err)
	}

	if err := docs.Upsert([]vector.PointStruct{{ID: "a", Vector: []float32{1, 0}}}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("pending:", docs.Pending())

	// Close flushes the buffered point, retires the collection handle, and
	// leaves bdb open for the caller.
	if err := docs.Close(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("collection closed:", docs.Closed())

	if err := store.Close(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("store closed:", store.Closed())

	// Output:
	// pending: 1
	// collection closed: true
	// store closed: true
}
