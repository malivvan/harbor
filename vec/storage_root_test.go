package vec

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func openRootTestStore(t *testing.T, root []byte) (*Store, *bolt.DB) {
	t.Helper()

	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "vec-root.db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open bbolt: %v", err)
	}
	t.Cleanup(func() {
		_ = bdb.Close()
	})

	store, err := Open(context.Background(), bdb, root, false, nil)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return store, bdb
}

func TestStoreRootScopesCollectionsAndMetadata(t *testing.T) {
	root := []byte("tenant")
	store, bdb := openRootTestStore(t, root)

	// The store must keep the root it was opened with.
	root[0] = 'X'

	if err := store.EnsureCollection("docs"); err != nil {
		t.Fatalf("ensure collection: %v", err)
	}
	meta := CollectionMeta{Name: "docs", VectorLen: 2, Metric: Cosine}
	if err := store.SaveCollectionMeta("docs", meta); err != nil {
		t.Fatalf("save metadata: %v", err)
	}
	if err := store.UpsertPoints("docs", []PointStruct{
		{ID: "p1", Vector: []float32{1, 2}},
	}); err != nil {
		t.Fatalf("upsert points: %v", err)
	}

	collections, err := store.ListCollections()
	if err != nil {
		t.Fatalf("list collections: %v", err)
	}
	if len(collections) != 1 || collections[0] != "docs" {
		t.Fatalf("expected [docs], got %v", collections)
	}

	loaded, err := store.LoadCollection("docs")
	if err != nil {
		t.Fatalf("load collection: %v", err)
	}
	if loaded["p1"] == nil {
		t.Fatal("expected point p1 to load")
	}

	gotMeta, err := store.LoadCollectionMeta("docs")
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	if gotMeta.Name != "docs" || gotMeta.VectorLen != 2 {
		t.Fatalf("unexpected metadata: %+v", gotMeta)
	}

	metas, err := store.ListCollectionMetas()
	if err != nil {
		t.Fatalf("list metadata: %v", err)
	}
	if len(metas) != 1 || metas[0].Name != "docs" {
		t.Fatalf("expected one docs metadata entry, got %+v", metas)
	}

	var rootBucket, collection, metaBucket, topCollection, topMeta *bolt.Bucket
	if err := bdb.View(func(tx *bolt.Tx) error {
		rootBucket = tx.Bucket([]byte("tenant"))
		topCollection = tx.Bucket([]byte("docs"))
		topMeta = tx.Bucket([]byte(collectionsMetaBucket))
		if rootBucket != nil {
			collection = rootBucket.Bucket([]byte("docs"))
			metaBucket = rootBucket.Bucket([]byte(collectionsMetaBucket))
		}
		return nil
	}); err != nil {
		t.Fatalf("inspect buckets: %v", err)
	}

	if rootBucket == nil {
		t.Fatal("expected root bucket tenant to exist")
	}
	if collection == nil {
		t.Fatal("expected tenant/docs bucket to exist")
	}
	if metaBucket == nil {
		t.Fatal("expected tenant/__collections_meta__ bucket to exist")
	}
	if topCollection != nil {
		t.Fatal("collection must not exist at the database root")
	}
	if topMeta != nil {
		t.Fatal("metadata must not exist at the database root")
	}

	if err := store.DropCollection("docs"); err != nil {
		t.Fatalf("drop collection: %v", err)
	}
	collections, err = store.ListCollections()
	if err != nil {
		t.Fatalf("list collections after drop: %v", err)
	}
	if len(collections) != 0 {
		t.Fatalf("expected no collections after drop, got %v", collections)
	}
	if _, err := store.LoadCollectionMeta("docs"); err == nil {
		t.Fatal("expected dropped collection metadata to be gone")
	}
}

func TestStoreRootsAreIsolated(t *testing.T) {
	bdbPath := filepath.Join(t.TempDir(), "vec-roots.db")
	bdb, err := bolt.Open(bdbPath, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open bbolt: %v", err)
	}
	defer bdb.Close()

	storeA, err := Open(context.Background(), bdb, []byte("a"), false, nil)
	if err != nil {
		t.Fatalf("open store a: %v", err)
	}
	storeB, err := Open(context.Background(), bdb, []byte("b"), false, nil)
	if err != nil {
		t.Fatalf("open store b: %v", err)
	}

	for name, store := range map[string]*Store{"a": storeA, "b": storeB} {
		if err := store.EnsureCollection("docs"); err != nil {
			t.Fatalf("%s: ensure collection: %v", name, err)
		}
		if err := store.UpsertPoints("docs", []PointStruct{
			{ID: name, Vector: []float32{1, 2}},
		}); err != nil {
			t.Fatalf("%s: upsert: %v", name, err)
		}
	}

	loadedA, err := storeA.LoadCollection("docs")
	if err != nil {
		t.Fatalf("load a: %v", err)
	}
	loadedB, err := storeB.LoadCollection("docs")
	if err != nil {
		t.Fatalf("load b: %v", err)
	}
	if len(loadedA) != 1 || loadedA["a"] == nil {
		t.Fatalf("unexpected store a points: %+v", loadedA)
	}
	if len(loadedB) != 1 || loadedB["b"] == nil {
		t.Fatalf("unexpected store b points: %+v", loadedB)
	}

	if err := bdb.View(func(tx *bolt.Tx) error {
		for _, root := range []string{"a", "b"} {
			bucket := tx.Bucket([]byte(root))
			if bucket == nil || bucket.Bucket([]byte("docs")) == nil {
				t.Errorf("expected %s/docs bucket", root)
			}
		}
		if tx.Bucket([]byte("docs")) != nil {
			t.Error("collection must not exist at the database root")
		}
		return nil
	}); err != nil {
		t.Fatalf("inspect roots: %v", err)
	}
}

func TestStoreRootMissing(t *testing.T) {
	store, bdb := openRootTestStore(t, []byte("absent"))

	collections, err := store.ListCollections()
	if err != nil {
		t.Fatalf("list collections: %v", err)
	}
	if len(collections) != 0 {
		t.Fatalf("expected no collections, got %v", collections)
	}

	points, err := store.LoadCollection("docs")
	if err != nil {
		t.Fatalf("load collection: %v", err)
	}
	if len(points) != 0 {
		t.Fatalf("expected no points, got %+v", points)
	}
	if _, err := store.LoadCollectionMeta("docs"); err == nil {
		t.Fatal("expected metadata lookup to fail when the root does not exist")
	}
	if err := store.DeletePoints("docs", []string{"p1"}); err != nil {
		t.Fatalf("delete points from missing collection: %v", err)
	}
	if err := store.DropCollection("docs"); err != nil {
		t.Fatalf("drop missing collection: %v", err)
	}

	if err := store.EnsureCollection("docs"); err != nil {
		t.Fatalf("ensure collection: %v", err)
	}
	if err := bdb.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte("absent")) == nil {
			t.Error("expected root bucket to be created")
		}
		return nil
	}); err != nil {
		t.Fatalf("inspect root: %v", err)
	}
}
