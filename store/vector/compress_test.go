package vector

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func compressPayload() string {
	return strings.Repeat("payload value that compresses well; ", 100)
}

func compressPoints() []PointStruct {
	return []PointStruct{
		{ID: "p1", Vector: []float32{1, 0, 0.5}, Payload: Payload{"body": compressPayload(), "n": 1}},
		{ID: "p2", Vector: []float32{0, 1, 0.25}, Payload: Payload{"body": compressPayload(), "n": 2}},
	}
}

func collectionBytes(t *testing.T, db *bolt.DB, name string) int {
	t.Helper()
	var total int
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(name))
		if b == nil {
			return nil
		}
		return b.ForEach(func(_, v []byte) error {
			if v != nil {
				total += len(v)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("read collection bucket: %v", err)
	}
	return total
}

func TestVectorCompressRoundTrip(t *testing.T) {
	ctx := context.Background()
	key := aeadKey(31)

	compressed := openTestStorageOptions(t, filepath.Join(t.TempDir(), "vec.db"), nil, Config{
		Secret:        key,
		EncryptPoints: true,
		Compress:      true,
		SharedDB:      true,
	})
	col, err := compressed.NewCollectionWithOptions(CollectionOptions{Name: "docs", VectorLen: 3, Metric: Cosine})
	if err != nil {
		t.Fatalf("NewCollection: %v", err)
	}
	if err := col.Upsert(compressPoints()); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	loaded, err := compressed.LoadCollection("docs")
	if err != nil {
		t.Fatalf("LoadCollection: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("loaded %d points, want 2", len(loaded))
	}
	if got, ok := loaded["p1"].Payload["body"].(string); !ok || got != compressPayload() {
		t.Fatalf("p1 payload = %v", loaded["p1"].Payload)
	}

	hits, err := col.Search([]float32{1, 0, 0.5}, nil, 1)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 1 || hits[0].ID != "p1" {
		t.Fatalf("Search = %+v", hits)
	}

	// Compare against an uncompressed store with the same points.
	plain := openTestStorageOptions(t, filepath.Join(t.TempDir(), "vec.db"), nil, Config{
		Secret:        key,
		EncryptPoints: true,
		SharedDB:      true,
	})
	plainCol, err := plain.NewCollectionWithOptions(CollectionOptions{Name: "docs", VectorLen: 3, Metric: Cosine})
	if err != nil {
		t.Fatalf("plain NewCollection: %v", err)
	}
	if err := plainCol.Upsert(compressPoints()); err != nil {
		t.Fatalf("plain Upsert: %v", err)
	}
	if err := plainCol.Flush(); err != nil {
		t.Fatalf("plain Flush: %v", err)
	}

	compressedSize := collectionBytes(t, compressed.db, "docs")
	plainSize := collectionBytes(t, plain.db, "docs")
	if compressedSize == 0 || compressedSize >= plainSize {
		t.Fatalf("compressed collection %d bytes, plain %d bytes", compressedSize, plainSize)
	}
	if err := ctx.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestVectorCompressQuantized(t *testing.T) {
	ctx := context.Background()
	store := openTestStorageOptions(t, filepath.Join(t.TempDir(), "vec.db"), nil, Config{
		Secret:        aeadKey(32),
		EncryptPoints: true,
		Compress:      true,
		UseQuant:      true,
		SharedDB:      true,
	})
	col, err := store.NewCollectionWithOptions(CollectionOptions{Name: "q", VectorLen: 4, Metric: Dot})
	if err != nil {
		t.Fatalf("NewCollection: %v", err)
	}
	points := []PointStruct{{ID: "a", Vector: []float32{0.1, 0.2, 0.3, 0.4}}}
	if err := col.Upsert(points); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	loaded, err := store.LoadCollection("q")
	if err != nil {
		t.Fatalf("LoadCollection: %v", err)
	}
	if len(loaded) != 1 || len(loaded["a"].Vector) != 4 {
		t.Fatalf("loaded = %+v", loaded)
	}
	if err := ctx.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestVectorCompressRotation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "vec.db")
	bdb, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	t.Cleanup(func() { _ = bdb.Close() })

	oldKey, newKey := aeadKey(33), aeadKey(34)
	store, err := Open(ctx, bdb, Config{Secret: oldKey, EncryptPoints: true, Compress: true, SharedDB: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	col, err := store.NewCollectionWithOptions(CollectionOptions{Name: "docs", VectorLen: 3, Metric: Cosine})
	if err != nil {
		t.Fatalf("NewCollection: %v", err)
	}
	if err := col.Upsert(compressPoints()); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := store.RotateAll(ctx, newKey); err != nil {
		t.Fatalf("RotateAll: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	again, err := Open(ctx, bdb, Config{Secret: newKey, EncryptPoints: true, SharedDB: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = again.Close() })
	loaded, err := again.LoadCollection("docs")
	if err != nil {
		t.Fatalf("LoadCollection after rotation: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("loaded %d points after rotation, want 2", len(loaded))
	}
	if got, _ := loaded["p2"].Payload["body"].(string); got != compressPayload() {
		t.Fatalf("payload after rotation = %q", got)
	}
	want := []float32{1, 0, 0.5}
	got := loaded["p1"].Vector
	if len(got) != len(want) {
		t.Fatalf("vector after rotation = %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("vector after rotation = %v", got)
		}
	}
}
