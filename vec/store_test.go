package vec

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/malivvan/binco"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestStorage(t *testing.T) {
	// Create a temporary file for testing
	tempFile, err := os.CreateTemp("", "storage-test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tempPath := tempFile.Name()
	tempFile.Close()
	defer os.Remove(tempPath)

	storage := openTestStorage(t, tempPath)
	defer storage.Close()

	// Test EnsureCollection
	err = storage.EnsureCollection("test-collection")
	if err != nil {
		t.Fatalf("Failed to ensure collection: %v", err)
	}

	// Test ListCollections
	collections, err := storage.ListCollections()
	if err != nil {
		t.Fatalf("Failed to list collections: %v", err)
	}
	if len(collections) != 1 || collections[0] != "test-collection" {
		t.Errorf("Expected [test-collection], got %v", collections)
	}

	// Test UpsertPoints with various payload types
	points := []PointStruct{
		{
			ID:     "1",
			Vector: []float32{1.0, 2.0, 3.0},
			Payload: map[string]interface{}{
				"string": "test1",
				"int":    int(42),
				"int64":  int64(100),
				"float":  float32(3.14),
				"double": float64(2.718),
				"bool":   true,
				"bytes":  []byte{0x01, 0x02, 0x03},
			},
		},
		{ID: "2", Vector: []float32{4.0, 5.0, 6.0}, Payload: map[string]interface{}{"name": "test2"}},
	}
	err = storage.UpsertPoints("test-collection", points)
	if err != nil {
		t.Fatalf("Failed to upsert points: %v", err)
	}

	// Test LoadCollection
	loadedPoints, err := storage.LoadCollection("test-collection")
	if err != nil {
		t.Fatalf("Failed to load collection: %v", err)
	}

	if len(loadedPoints) != 2 {
		t.Errorf("Expected 2 points, got %d", len(loadedPoints))
	}

	p1 := loadedPoints["1"]
	if p1 == nil {
		t.Fatal("Expected point 1 to exist")
	}

	// Verify payload types and values
	expectedPayload := map[string]interface{}{
		"string": "test1",
		"int":    int64(42),     // integer payloads decode as int64
		"int64":  int64(100),    // integer payloads decode as int64
		"float":  float64(3.14), // float payloads decode as float64
		"double": float64(2.718),
		"bool":   true,
		"bytes":  []byte{0x01, 0x02, 0x03},
	}

	for k, v := range expectedPayload {
		if val, ok := p1.Payload[k]; !ok {
			t.Errorf("Expected payload key %s to exist", k)
		} else {
			if k == "float" {
				// Special handling for float32 precision when stored as float64
				if float32(val.(float64)) != 3.14 {
					t.Errorf("Expected payload key %s to be 3.14, got %v", k, val)
				}
			} else if !reflect.DeepEqual(val, v) {
				t.Errorf("Expected payload key %s to be %v (%T), got %v (%T)", k, v, v, val, val)
			}
		}
	}

	if loadedPoints["2"] == nil {
		t.Error("Expected point 2 to exist")
	}

	// Test DeletePoints
	err = storage.DeletePoints("test-collection", []string{"1"})
	if err != nil {
		t.Fatalf("Failed to delete points: %v", err)
	}

	// Test LoadCollection after deletion
	loadedPoints, err = storage.LoadCollection("test-collection")
	if err != nil {
		t.Fatalf("Failed to load collection after deletion: %v", err)
	}

	if len(loadedPoints) != 1 {
		t.Errorf("Expected 1 point after deletion, got %d", len(loadedPoints))
	}

	if loadedPoints["1"] != nil {
		t.Error("Expected point 1 to be deleted")
	}

	if loadedPoints["2"] == nil {
		t.Error("Expected point 2 to still exist")
	}

	// Test ListCollections with multiple
	err = storage.EnsureCollection("another-one")
	if err != nil {
		t.Fatalf("Failed to ensure collection: %v", err)
	}
	collections, err = storage.ListCollections()
	if err != nil {
		t.Fatalf("Failed to list collections: %v", err)
	}
	if len(collections) != 2 {
		t.Errorf("Expected 2 collections, got %d", len(collections))
	}

	// Test Close and closed errors
	storage.Close()
	if _, err := storage.ListCollections(); err == nil {
		t.Error("Expected error calling ListCollections on closed storage")
	}
	if err := storage.EnsureCollection("test"); err == nil {
		t.Error("Expected error calling EnsureCollection on closed storage")
	}
	if err := storage.UpsertPoints("test", nil); err == nil {
		t.Error("Expected error calling UpsertPoints on closed storage")
	}
	if _, err := storage.LoadCollection("test"); err == nil {
		t.Error("Expected error calling LoadCollection on closed storage")
	}
	if err := storage.DeletePoints("test", nil); err == nil {
		t.Error("Expected error calling DeletePoints on closed storage")
	}
}

func TestStorageWithQuantization(t *testing.T) {
	tempFile, _ := os.CreateTemp("", "storage-quant-test")
	tempPath := tempFile.Name()
	tempFile.Close()
	defer os.Remove(tempPath)

	storage := openTestStorageOptions(t, tempPath, nil, Config{UseQuant: true})
	defer storage.Close()

	colName := "quantized-col"
	storage.EnsureCollection(colName)

	points := []PointStruct{
		{ID: "1", Vector: []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0}},
	}

	err := storage.UpsertPoints(colName, points)
	if err != nil {
		t.Fatalf("Failed to upsert points: %v", err)
	}

	loadedPoints, err := storage.LoadCollection(colName)
	if err != nil {
		t.Fatalf("Failed to load collection: %v", err)
	}

	p1, ok := loadedPoints["1"]
	if !ok {
		t.Fatal("Point 1 not found")
	}

	if len(p1.Vector) != 8 {
		t.Errorf("Expected vector length 8, got %d", len(p1.Vector))
	}

	// Verify that the vector is restored (it won't be exactly the same due to SQ8 quantization)
	for i, v := range p1.Vector {
		if v < points[0].Vector[i]-0.5 || v > points[0].Vector[i]+0.5 {
			t.Errorf("Vector element %d significantly different: got %f, expected around %f", i, v, points[0].Vector[i])
		}
	}
}

func TestCollectionMetaStorage(t *testing.T) {
	tempFile, _ := os.CreateTemp("", "storage-meta-test")
	tempPath := tempFile.Name()
	tempFile.Close()
	defer os.Remove(tempPath)

	storage := openTestStorage(t, tempPath)
	defer storage.Close()

	meta := CollectionMeta{
		Name:      "test-meta",
		VectorLen: 128,
		Metric:    Cosine,
		UseHNSW:   true,
		HNSWParams: HNSWParams{
			M:              16,
			EfConstruction: 200,
		},
	}

	// Test SaveCollectionMeta
	err := storage.SaveCollectionMeta(meta.Name, meta)
	if err != nil {
		t.Fatalf("Failed to save collection meta: %v", err)
	}

	// Test LoadCollectionMeta
	loadedMeta, err := storage.LoadCollectionMeta(meta.Name)
	if err != nil {
		t.Fatalf("Failed to load collection meta: %v", err)
	}

	if !reflect.DeepEqual(*loadedMeta, meta) {
		t.Errorf("Expected meta %v, got %v", meta, *loadedMeta)
	}

	// Test ListCollectionMetas
	metas, err := storage.ListCollectionMetas()
	if err != nil {
		t.Fatalf("Failed to list collection metas: %v", err)
	}

	if len(metas) != 1 {
		t.Errorf("Expected 1 meta, got %d", len(metas))
	}

	if !reflect.DeepEqual(metas[0], meta) {
		t.Errorf("Expected meta %v, got %v", meta, metas[0])
	}

	// Test LoadCollectionMeta for non-existent collection
	_, err = storage.LoadCollectionMeta("non-existent")
	if err == nil {
		t.Error("Expected error loading non-existent meta")
	}

	// Test ListCollectionMetas when empty (it was not empty, but we can test it with a fresh storage)
}

func TestStorageErrorCases(t *testing.T) {
	tempFile, _ := os.CreateTemp("", "storage-error-test")
	tempPath := tempFile.Name()
	tempFile.Close()
	defer os.Remove(tempPath)

	storage := openTestStorage(t, tempPath)

	// Test LoadCollectionMeta when bucket doesn't exist
	_, err := storage.LoadCollectionMeta("any")
	if err == nil {
		t.Error("Expected error when metadata bucket doesn't exist")
	}

	// Test ListCollectionMetas when bucket doesn't exist
	metas, err := storage.ListCollectionMetas()
	if err != nil {
		t.Fatalf("ListCollectionMetas failed: %v", err)
	}
	if len(metas) != 0 {
		t.Errorf("Expected 0 metas, got %d", len(metas))
	}

	storage.Close()
}

// TestToProtoPointInvalidUTF8 verifies that invalid UTF-8 strings in payload
// are sanitized (replaced with U+FFFD) before binco encoding.
// This covers the P1 fix: UTF-8 validation in toStoredPoint.
func TestToProtoPointInvalidUTF8(t *testing.T) {
	t.Run("InvalidUTF8String_Sanitized", func(t *testing.T) {
		// Create a string with embedded invalid UTF-8 bytes (e.g., from binary file content)
		invalidStr := "hello" + string([]byte{0x80, 0xfe, 0xff}) + "world"

		p := &PointStruct{
			ID:     "utf8-test",
			Vector: []float32{1.0, 2.0},
			Payload: map[string]interface{}{
				"content": invalidStr,
				"title":   "正常标题",
				"binary":  string([]byte{0xc0, 0xaf}), // overlong encoding
			},
		}

		converted, err := toStoredPoint(p)
		if err != nil || converted == nil {
			t.Fatalf("toStoredPoint returned nil, err=%v", err)
		}

		// Verify all string values are valid UTF-8
		for k, v := range converted.Payload {
			if sv := v.GetStringValue(); sv != "" {
				if !utf8.ValidString(sv) {
					t.Errorf("payload key %q still contains invalid UTF-8 after sanitization", k)
				}
			}
		}
	})

	t.Run("ValidUTF8_PassThrough", func(t *testing.T) {
		p := &PointStruct{
			ID:     "valid-test",
			Vector: []float32{1.0},
			Payload: map[string]interface{}{
				"text": "hello 世界 🌍",
				"num":  int(42),
			},
		}

		converted, err := toStoredPoint(p)
		if err != nil || converted == nil {
			t.Fatalf("toStoredPoint returned nil, err=%v", err)
		}

		if got := converted.Payload["text"].GetStringValue(); got != "hello 世界 🌍" {
			t.Errorf("valid UTF-8 should pass through unchanged, got %q", got)
		}
	})

	t.Run("RoundTrip_InvalidUTF8_EncodeSucceeds", func(t *testing.T) {
		// The real bug: encoding would fail on invalid UTF-8
		invalidStr := "data\x80corrupted"
		p := &PointStruct{
			ID:     "marshal-test",
			Vector: []float32{1.0},
			Payload: map[string]interface{}{
				"raw": invalidStr,
			},
		}

		converted, err := toStoredPoint(p)
		if err != nil {
			t.Fatalf("toStoredPoint: %v", err)
		}
		data := binco.Encode(StoredPointBinco, *converted)
		if len(data) == 0 {
			t.Fatal("encoded data is empty")
		}

		// Round-trip: decode and verify content is valid UTF-8
		decoded, err := binco.DecodeExact(StoredPointBinco, data)
		if err != nil {
			t.Fatalf("binco decode failed: %v", err)
		}
		content := decoded.Payload["raw"].GetStringValue()
		if !utf8.ValidString(content) {
			t.Errorf("round-trip payload still has invalid UTF-8: %q (hex: %x)", content, []byte(content))
		}
	})
}

func openRootTestStore(t *testing.T, root []byte) (*Store, *bolt.DB) {
	t.Helper()

	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "vec-root.db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open bbolt: %v", err)
	}
	t.Cleanup(func() {
		_ = bdb.Close()
	})

	store, err := openStore(context.Background(), bdb, root, Config{})
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

	storeA, err := openStore(context.Background(), bdb, []byte("a"), Config{})
	if err != nil {
		t.Fatalf("open store a: %v", err)
	}
	storeB, err := openStore(context.Background(), bdb, []byte("b"), Config{})
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

func TestNestedRoot(t *testing.T) {
	root := [][]byte{[]byte("tenant"), []byte("app")}
	store, bdb := aeadOpenStore(t, filepath.Join(t.TempDir(), "nested.db"), false, Config{Root: root})
	require.NoError(t, store.EnsureCollection("docs"))
	require.NoError(t, store.UpsertPoints("docs", aeadTestPoints()))

	require.NoError(t, bdb.View(func(tx *bolt.Tx) error {
		tenant := tx.Bucket([]byte("tenant"))
		require.NotNil(t, tenant)
		app := tenant.Bucket([]byte("app"))
		require.NotNil(t, app)
		require.NotNil(t, app.Bucket([]byte("docs")))
		return nil
	}))
}
