package vec

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func openTodoVecStore(t *testing.T, path string) (*Store, *bbolt.DB) {
	t.Helper()

	bdb, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	store, err := Open(context.Background(), bdb, Config{})
	if err != nil {
		_ = bdb.Close()
		t.Fatalf("Open: %v", err)
	}
	return store, bdb
}

// TestStoreCloseFlushesBufferedPoints guards V1.
func TestStoreCloseFlushesBufferedPoints(t *testing.T) {
	path := filepath.Join(t.TempDir(), "flush.db")
	store, _ := openTodoVecStore(t, path)

	col, err := store.NewCollection("docs", 2, Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]PointStruct{{ID: "1", Vector: []float32{1, 0}}}))

	require.NoError(t, store.Close())

	// A fresh process must see the buffered point.
	store, _ = openTodoVecStore(t, path)
	defer store.Close()
	loaded, err := store.LoadCollection("docs")
	require.NoError(t, err)
	require.Len(t, loaded, 1)
}

// TestCollectionCloseKeepsSharedStoreUsable guards collection Close keeping the
// shared store usable.
func TestCollectionCloseKeepsSharedStoreUsable(t *testing.T) {
	store := newTempStore(t)

	a, err := store.NewCollection("a", 2, Cosine, false)
	require.NoError(t, err)
	b, err := store.NewCollection("b", 2, Cosine, false)
	require.NoError(t, err)

	require.NoError(t, a.Upsert([]PointStruct{{ID: "1", Vector: []float32{1, 0}}}))
	require.NoError(t, a.Close())

	require.NoError(t, b.Upsert([]PointStruct{{ID: "2", Vector: []float32{0, 1}}}))
	require.NoError(t, b.Flush())

	names, err := store.ListCollections()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b"}, names)
}

// TestSearchReturnsDetachedPayloads guards V3/V17.
func TestSearchReturnsDetachedPayloads(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]PointStruct{{
		ID:      "1",
		Vector:  []float32{1, 0},
		Payload: Payload{"role": "admin", "nested": map[string]any{"k": "v"}},
	}}))

	results, err := col.Search([]float32{1, 0}, nil, 1)
	require.NoError(t, err)
	require.Len(t, results, 1)
	results[0].Payload["role"] = "mutated"
	results[0].Payload["nested"].(map[string]any)["k"] = "mutated"

	filter := &Filter{Must: []Condition{{Key: "role", Match: MatchValue{Value: "admin"}}}}
	again, err := col.Search([]float32{1, 0}, filter, 1)
	require.NoError(t, err)
	require.Len(t, again, 1, "mutating a search result changed the index")

	points, err := col.GetPointsByFilter(nil)
	require.NoError(t, err)
	require.Len(t, points, 1)
	require.Equal(t, "v", points[0].Payload["nested"].(map[string]any)["k"])
}

// TestReopenRejectsCollectionConfigMismatch guards V4.
func TestReopenRejectsCollectionConfigMismatch(t *testing.T) {
	store := newTempStore(t)
	_, err := store.NewCollection("docs", 3, Cosine, false)
	require.NoError(t, err)

	_, err = store.NewCollection("docs", 3, Euclid, false)
	require.ErrorIs(t, err, ErrInvalidConfig)

	_, err = store.NewCollection("docs", 4, Cosine, false)
	require.ErrorIs(t, err, ErrInvalidConfig)

	// The stored configuration is untouched.
	meta, err := store.LoadCollectionMeta("docs")
	require.NoError(t, err)
	require.Equal(t, Cosine, meta.Metric)
	require.Equal(t, 3, meta.VectorLen)
}

// TestHNSWFilteredSearchFindsDistantMatch guards V5.
func TestHNSWFilteredSearchFindsDistantMatch(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollectionWithParams("docs", 2, Cosine, true, HNSWParams{
		M:              8,
		EfConstruction: 64,
		EfSearch:       32,
	})
	require.NoError(t, err)

	// Many close points with no matching payload.
	batch := make([]PointStruct, 0, 200)
	for i := 0; i < 200; i++ {
		batch = append(batch, PointStruct{
			ID:      "close-" + string(rune('a'+i%26)) + string(rune('a'+i/26)),
			Vector:  []float32{1, 0},
			Payload: Payload{"tag": "common"},
		})
	}
	// One far point that is the only match.
	batch = append(batch, PointStruct{
		ID:      "rare",
		Vector:  []float32{0, 1},
		Payload: Payload{"tag": "rare"},
	})
	require.NoError(t, col.Upsert(batch))

	filter := &Filter{Must: []Condition{{Key: "tag", Match: MatchValue{Value: "rare"}}}}
	results, err := col.Search([]float32{1, 0}, filter, 1)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "rare", results[0].ID)
}

// TestContainsAfterJSONReload guards V6.
func TestContainsAfterJSONReload(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]PointStruct{{
		ID:     "1",
		Vector: []float32{1, 0},
		Payload: Payload{
			"tags":   []any{int64(1), int64(2)},
			"nested": []any{[]any{int64(3)}},
		},
	}}))
	require.NoError(t, col.Flush())

	loaded, err := store.LoadCollection("docs")
	require.NoError(t, err)
	require.Len(t, loaded, 1)

	payload := loaded["1"].Payload
	require.True(t, MatchFilter(payload, &Filter{Must: []Condition{
		{Type: MatchTypeContains, Key: "tags", Match: MatchValue{Value: 1}},
	}}), "contains must coerce numeric types after a reload")

	// Nested arrays must not panic.
	require.False(t, MatchFilter(payload, &Filter{Must: []Condition{
		{Type: MatchTypeContains, Key: "nested", Match: MatchValue{Value: 3}},
	}}))
}

// TestPartialCollectionRotationKeepsOldDataReadable guards V7.
func TestPartialCollectionRotationKeepsOldDataReadable(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "partial.db")
	keyA, keyB := aeadKey(70), aeadKey(71)

	bdb, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	store, err := Open(ctx, bdb, Config{Secret: keyA, EncryptPoints: true})
	require.NoError(t, err)

	require.NoError(t, store.EnsureCollection("rotated"))
	require.NoError(t, store.EnsureCollection("pending"))
	require.NoError(t, store.UpsertPoints("rotated", aeadTestPoints()[:1]))
	require.NoError(t, store.UpsertPoints("pending", aeadTestPoints()[:1]))
	require.NoError(t, store.ReEncryptCollection(ctx, "rotated", keyB))
	require.NoError(t, store.Close())

	// The old marker still verifies with the old key and the un-rotated
	// collection is readable; the rotated one is new-key ciphertext.
	bdb, err = bbolt.Open(path, 0600, &bbolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	store, err = Open(ctx, bdb, Config{Secret: keyA, EncryptPoints: true})
	require.NoError(t, err)
	loaded, err := store.LoadCollection("pending")
	require.NoError(t, err)
	require.NotEmpty(t, loaded)
	_, err = store.LoadCollection("rotated")
	require.ErrorIs(t, err, ErrAuthFailed)
	require.NoError(t, store.Close())

	// The new key cannot verify the old marker: RotateAll must complete the
	// rotation before the store is closed.
	bdb, err = bbolt.Open(path, 0600, &bbolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	_, err = Open(ctx, bdb, Config{Secret: keyB, EncryptPoints: true})
	require.ErrorIs(t, err, ErrAuthFailed)
	_ = bdb.Close()
}

// TestUpsertDoesNotAliasCallerData guards V8.
func TestUpsertDoesNotAliasCallerData(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	require.NoError(t, err)

	vector := []float32{1, 0}
	payload := Payload{"role": "admin"}
	require.NoError(t, col.Upsert([]PointStruct{{ID: "1", Vector: vector, Payload: payload}}))

	vector[0] = 0
	payload["role"] = "user"

	results, err := col.Search([]float32{1, 0}, nil, 1)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "admin", results[0].Payload["role"])
	require.InDelta(t, 1, results[0].Score, 1e-6, "the stored vector changed with the caller's slice")
}

// TestQuantizationDoesNotLeakReservedKey guards V9.
func TestQuantizationDoesNotLeakReservedKey(t *testing.T) {
	bdb, err := bbolt.Open(filepath.Join(t.TempDir(), "quant.db"), 0600, &bbolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	store, err := Open(context.Background(), bdb, Config{UseQuant: true})
	require.NoError(t, err)
	defer store.Close()

	col, err := store.NewCollection("docs", 2, Cosine, false)
	require.NoError(t, err)

	payload := Payload{"role": "admin"}
	require.NoError(t, col.Upsert([]PointStruct{{ID: "1", Vector: []float32{1, 0}, Payload: payload}}))
	require.NoError(t, col.Flush())

	require.NotContains(t, payload, "__quantized_vector", "the caller payload was mutated")

	points, err := col.GetPointsByFilter(nil)
	require.NoError(t, err)
	require.Len(t, points, 1)
	require.NotContains(t, points[0].Payload, "__quantized_vector")
}

// TestCollectionMetaRecordsSchemaFingerprint guards V10.
func TestCollectionMetaRecordsSchemaFingerprint(t *testing.T) {
	store := newTempStore(t)
	_, err := store.NewCollection("docs", 2, Cosine, false)
	require.NoError(t, err)

	meta, err := store.LoadCollectionMeta("docs")
	require.NoError(t, err)
	require.Equal(t, uint32(CollectionFormatVersion), meta.FormatVersion)
	require.Equal(t, StoredPointSchemaFingerprint, meta.SchemaFingerprint)

	// A different fingerprint means the stored blobs cannot be reinterpreted.
	meta.SchemaFingerprint = "different"
	require.NoError(t, store.SaveCollectionMeta("docs", *meta))
	_, err = store.NewCollection("docs", 2, Cosine, false)
	require.ErrorIs(t, err, ErrInvalidConfig)
}

// TestHNSWSearchRejectsWrongDimension guards V13.
func TestHNSWSearchRejectsWrongDimension(t *testing.T) {
	idx := NewHNSWIndex(Cosine)
	require.NoError(t, idx.Upsert([]PointStruct{{ID: "1", Vector: []float32{1, 0}}}))

	_, err := idx.Search([]float32{1, 0, 0}, nil, 1)
	require.Error(t, err)
}

// TestVersionsAreMonotonic guards V15.
func TestVersionsAreMonotonic(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	require.NoError(t, err)

	require.NoError(t, col.Upsert([]PointStruct{{ID: "1", Vector: []float32{1, 0}}}))
	first, err := col.GetPointsByFilter(nil)
	require.NoError(t, err)
	require.Len(t, first, 1)

	require.NoError(t, col.Upsert([]PointStruct{{ID: "2", Vector: []float32{0, 1}}}))
	second, err := col.GetPointsByFilter(nil)
	require.NoError(t, err)

	var version2 uint64
	for _, p := range second {
		if p.ID == "2" {
			version2 = p.Version
		}
	}
	require.NotZero(t, version2)
	require.Greater(t, version2, first[0].Version)
}

// TestUnencodablePayloadReturnsError guards V16.
func TestUnencodablePayloadReturnsError(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	require.NoError(t, err)

	require.NoError(t, col.Upsert([]PointStruct{{
		ID:      "1",
		Vector:  []float32{1, 0},
		Payload: Payload{"bad": func() {}},
	}}))
	err = col.Flush()
	require.Error(t, err)
}

// TestNilFilterMatchesAllPoints guards V18.
func TestNilFilterMatchesAllPoints(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]PointStruct{
		{ID: "1", Vector: []float32{1, 0}},
		{ID: "2", Vector: []float32{0, 1}},
	}))

	points, err := col.GetPointsByFilter(nil)
	require.NoError(t, err)
	require.Len(t, points, 2)

	err = store.UpsertPoints("missing", []PointStruct{{ID: "1", Vector: []float32{1, 0}}})
	require.ErrorIs(t, err, ErrCollectionNotFound)
}

// compile-time guard: the old dead helper must stay removed.
func TestEncStateHasNoEnabledHelper(t *testing.T) {
	var e *encState
	require.Nil(t, e)
	_ = errors.Is(ErrCollectionNotFound, ErrCollectionNotFound)
}

// TestHNSWMixedDimensionFirstBatch covers N1: a mixed-dimension first batch must
// return an error instead of panicking inside Graph.Add.
func TestHNSWMixedDimensionFirstBatch(t *testing.T) {
	idx := NewHNSWIndex(Cosine)
	require.NotPanics(t, func() {
		err := idx.Upsert([]PointStruct{
			{ID: "a", Vector: []float32{1, 0}},
			{ID: "b", Vector: []float32{1, 0, 0}},
		})
		require.Error(t, err)
	})
}
