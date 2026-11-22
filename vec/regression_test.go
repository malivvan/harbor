package vec

import (
	"context"
	"math"
	"strings"
	"sync"
	"testing"

	"go.etcd.io/bbolt"
)

// newTempStore returns a Store backed by a temporary bbolt database.
func newTempStore(t *testing.T) *Store {
	t.Helper()

	bdb, err := bbolt.Open(t.TempDir()+"/regression.db", 0600, &bbolt.Options{Timeout: 1e9})
	if err != nil {
		t.Fatalf("open bbolt: %v", err)
	}
	store, err := Open(context.Background(), bdb, Config{})
	if err != nil {
		_ = bdb.Close()
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// TestSearchRejectsNonPositiveTopK is a regression test for topK <= 0 being
// unvalidated: negative values panicked (slice bounds / make with negative
// capacity) and HNSW returned a result for topK == 0.
func TestSearchRejectsNonPositiveTopK(t *testing.T) {
	t.Run("flat index", func(t *testing.T) {
		idx := NewFlatIndex(Cosine)
		if err := idx.Upsert([]PointStruct{{ID: "1", Vector: []float32{1, 0}}}); err != nil {
			t.Fatalf("upsert: %v", err)
		}
		for _, topK := range []int{0, -1} {
			if _, err := idx.Search([]float32{1, 0}, nil, topK); err == nil {
				t.Fatalf("FlatIndex.Search with topK=%d returned no error", topK)
			}
		}
	})

	t.Run("hnsw index", func(t *testing.T) {
		idx := NewHNSWIndex(Cosine)
		if err := idx.Upsert([]PointStruct{{ID: "1", Vector: []float32{1, 0}}}); err != nil {
			t.Fatalf("upsert: %v", err)
		}
		for _, topK := range []int{0, -1} {
			if _, err := idx.Search([]float32{1, 0}, nil, topK); err == nil {
				t.Fatalf("HNSWIndex.Search with topK=%d returned no error", topK)
			}
		}
	})

	t.Run("collection", func(t *testing.T) {
		var store *Store
		col, err := store.NewCollection("topk", 2, Cosine, false)
		if err != nil {
			t.Fatalf("new collection: %v", err)
		}
		if err := col.Upsert([]PointStruct{{ID: "1", Vector: []float32{1, 0}}}); err != nil {
			t.Fatalf("upsert: %v", err)
		}
		for _, topK := range []int{0, -1} {
			if _, err := col.Search([]float32{1, 0}, nil, topK); err == nil {
				t.Fatalf("Collection.Search with topK=%d returned no error", topK)
			}
		}
	})
}

// TestVersionPersistsAndIsReturned is a regression test for PointStruct.Version
// being dropped by toStoredPoint and left at zero in search results.
func TestVersionPersistsAndIsReturned(t *testing.T) {
	store := newTempStore(t)

	if err := store.EnsureCollection("versions"); err != nil {
		t.Fatalf("ensure collection: %v", err)
	}
	points := []PointStruct{
		{ID: "1", Version: 42, Vector: []float32{1, 0}},
		{ID: "2", Version: 43, Vector: []float32{0, 1}},
	}
	if err := store.UpsertPoints("versions", points); err != nil {
		t.Fatalf("upsert points: %v", err)
	}

	loaded, err := store.LoadCollection("versions")
	if err != nil {
		t.Fatalf("load collection: %v", err)
	}
	if got := loaded["1"].Version; got != 42 {
		t.Fatalf("loaded version = %d, want 42", got)
	}

	col, err := store.NewCollection("versions", 2, Cosine, false)
	if err != nil {
		t.Fatalf("new collection: %v", err)
	}
	results, err := col.Search([]float32{1, 0}, nil, 1)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("search returned %d results, want 1", len(results))
	}
	if results[0].Version != 42 {
		t.Fatalf("search result version = %d, want 42", results[0].Version)
	}

	// Index level upserts keep the version they are given and report it back.
	for name, build := range map[string]func() VectorIndex{
		"flat": func() VectorIndex { return NewFlatIndex(Cosine) },
		"hnsw": func() VectorIndex { return NewHNSWIndex(Cosine) },
	} {
		t.Run(name, func(t *testing.T) {
			idx := build()
			if err := idx.Upsert([]PointStruct{{ID: "7", Version: 99, Vector: []float32{1, 0}}}); err != nil {
				t.Fatalf("upsert: %v", err)
			}
			results, err := idx.Search([]float32{1, 0}, nil, 1)
			if err != nil {
				t.Fatalf("search: %v", err)
			}
			if len(results) != 1 || results[0].Version != 99 {
				t.Fatalf("search result = %+v, want version 99", results)
			}
		})
	}
}

// TestPayloadRoundTripTypes is a regression test for fromStoredPoint decoding
// raw byte payloads as JSON (turning []byte("123") into float64(123)) and for
// JSON-serialized maps losing their integer types.
func TestPayloadRoundTripTypes(t *testing.T) {
	store := newTempStore(t)

	if err := store.EnsureCollection("payloads"); err != nil {
		t.Fatalf("ensure collection: %v", err)
	}

	points := []PointStruct{
		{
			ID:     "1",
			Vector: []float32{1, 0},
			Payload: Payload{
				"raw":    []byte("123"),          // would decode to float64(123)
				"quoted": []byte(`"x"`),          // would decode to "x"
				"nested": map[string]any{"n": 7}, // nested integers became float64
				"list":   []any{1, 2.5, "three"},
			},
		},
	}
	if err := store.UpsertPoints("payloads", points); err != nil {
		t.Fatalf("upsert points: %v", err)
	}

	loaded, err := store.LoadCollection("payloads")
	if err != nil {
		t.Fatalf("load collection: %v", err)
	}
	payload := loaded["1"].Payload

	raw, ok := payload["raw"].([]byte)
	if !ok || string(raw) != "123" {
		t.Fatalf("raw bytes payload = %#v (%T), want []byte(\"123\")", payload["raw"], payload["raw"])
	}
	quoted, ok := payload["quoted"].([]byte)
	if !ok || string(quoted) != `"x"` {
		t.Fatalf("quoted bytes payload = %#v (%T), want the raw bytes", payload["quoted"], payload["quoted"])
	}

	nested, ok := payload["nested"].(map[string]any)
	if !ok {
		t.Fatalf("nested payload = %#v (%T), want a map", payload["nested"], payload["nested"])
	}
	if got, ok := nested["n"].(int64); !ok || got != 7 {
		t.Fatalf("nested integer = %#v (%T), want int64(7)", nested["n"], nested["n"])
	}

	list, ok := payload["list"].([]any)
	if !ok || len(list) != 3 {
		t.Fatalf("list payload = %#v (%T), want 3 elements", payload["list"], payload["list"])
	}
	if got, ok := list[0].(int64); !ok || got != 1 {
		t.Fatalf("list[0] = %#v (%T), want int64(1)", list[0], list[0])
	}
	if got, ok := list[1].(float64); !ok || got != 2.5 {
		t.Fatalf("list[1] = %#v (%T), want float64(2.5)", list[1], list[1])
	}
}

// TestFilterMatchesAfterReload is a regression test for exact filters on
// persisted numeric payloads: an int payload comes back as int64, so a filter
// built with an int used to stop matching after a restart.
func TestFilterMatchesAfterReload(t *testing.T) {
	store := newTempStore(t)

	if err := store.EnsureCollection("filters"); err != nil {
		t.Fatalf("ensure collection: %v", err)
	}
	points := []PointStruct{
		{ID: "1", Vector: []float32{1, 0}, Payload: Payload{"n": 5, "f": float32(1.5)}},
		{ID: "2", Vector: []float32{0, 1}, Payload: Payload{"n": 50, "f": float32(7.5)}},
	}
	if err := store.UpsertPoints("filters", points); err != nil {
		t.Fatalf("upsert points: %v", err)
	}

	col, err := store.NewCollection("filters", 2, Cosine, false)
	if err != nil {
		t.Fatalf("new collection: %v", err)
	}

	exact := &Filter{Must: []Condition{{Key: "n", Type: MatchTypeExact, Match: MatchValue{Value: 5}}}}
	got, err := col.Search([]float32{1, 0}, exact, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 1 || got[0].ID != "1" {
		t.Fatalf("exact filter after reload returned %v, want point 1", got)
	}

	rng := &Filter{Must: []Condition{{Key: "n", Type: MatchTypeRange, Range: &RangeValue{GTE: 10}}}}
	got, err = col.Search([]float32{1, 0}, rng, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 1 || got[0].ID != "2" {
		t.Fatalf("range filter after reload returned %v, want point 2", got)
	}

	floatExact := &Filter{Must: []Condition{{Key: "f", Type: MatchTypeExact, Match: MatchValue{Value: float64(1.5)}}}}
	got, err = col.Search([]float32{1, 0}, floatExact, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 1 || got[0].ID != "1" {
		t.Fatalf("float exact filter after reload returned %v, want point 1", got)
	}
}

// TestMatchExactNumericEquality covers matchExact directly.
func TestMatchExactNumericEquality(t *testing.T) {
	cases := []struct {
		name string
		val  any
		want any
		ok   bool
	}{
		{"int vs int64", 5, int64(5), true},
		{"int64 vs int", int64(5), 5, true},
		{"float64 vs int", float64(5), 5, true},
		{"float32 vs float64", float32(1.5), float64(1.5), true},
		{"different numbers", 5, 6, false},
		{"number vs string", 5, "5", false},
		{"strings", "a", "a", true},
		{"slices", []string{"a"}, []string{"a"}, true},
		{"different slices", []string{"a"}, []string{"b"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := matchExact(tc.val, tc.want); got != tc.ok {
				t.Fatalf("matchExact(%#v, %#v) = %v, want %v", tc.val, tc.want, got, tc.ok)
			}
		})
	}
}

// TestNewCollectionKeepsMetadataOnLoadFailure is a regression test for
// SaveCollectionMeta running before the loaded points are validated: opening an
// existing collection with the wrong dimension used to overwrite the metadata
// stored on disk.
func TestNewCollectionKeepsMetadataOnLoadFailure(t *testing.T) {
	store := newTempStore(t)

	colName := "meta-guard"
	if err := store.EnsureCollection(colName); err != nil {
		t.Fatalf("ensure collection: %v", err)
	}
	if err := store.UpsertPoints(colName, []PointStruct{{ID: "1", Vector: []float32{1, 2}}}); err != nil {
		t.Fatalf("upsert points: %v", err)
	}
	original := CollectionMeta{Name: colName, VectorLen: 2, Metric: Cosine}
	if err := store.SaveCollectionMeta(colName, original); err != nil {
		t.Fatalf("save metadata: %v", err)
	}

	if _, err := store.NewCollection(colName, 3, Euclid, false); err == nil {
		t.Fatal("expected NewCollection to reject the dimension mismatch")
	}

	meta, err := store.LoadCollectionMeta(colName)
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	if meta.VectorLen != 2 || meta.Metric != Cosine {
		t.Fatalf("metadata was overwritten: %+v, want %+v", meta, original)
	}
}

// TestHNSWParamsAreApplied is a regression test for EfConstruction and K being
// silently ignored by NewHNSWIndexWithParams.
func TestHNSWParamsAreApplied(t *testing.T) {
	idx := NewHNSWIndexWithParams(Cosine, HNSWParams{
		M:              8,
		EfConstruction: 77,
		EfSearch:       12,
		K:              5,
	})

	if idx.graph.M != 8 {
		t.Fatalf("graph.M = %d, want 8", idx.graph.M)
	}
	if idx.graph.EfConstruction != 77 {
		t.Fatalf("graph.EfConstruction = %d, want 77", idx.graph.EfConstruction)
	}
	if idx.graph.EfSearch != 12 {
		t.Fatalf("graph.EfSearch = %d, want 12", idx.graph.EfSearch)
	}
	if idx.params.K != 5 {
		t.Fatalf("params.K = %d, want 5", idx.params.K)
	}

	// Non-positive values fall back to the defaults instead of being ignored.
	idx = NewHNSWIndexWithParams(Cosine, HNSWParams{M: -1, EfConstruction: -1, EfSearch: -1, K: -1})
	def := DefaultHNSWParams()
	if idx.graph.M != def.M || idx.graph.EfConstruction != def.EfConstruction ||
		idx.graph.EfSearch != def.EfSearch || idx.params.K != def.K {
		t.Fatalf("invalid params were not normalized: %+v", idx.params)
	}
}

// TestHNSWUpsertRejectsDimensionMismatch is a regression test for Upsert
// mutating the points map before Graph.Add could fail on a dimension mismatch,
// leaving IDs in the map that the graph does not know about.
func TestHNSWUpsertRejectsDimensionMismatch(t *testing.T) {
	idx := NewHNSWIndex(Cosine)

	if err := idx.Upsert([]PointStruct{{ID: "1", Vector: []float32{1, 0}}}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	err := idx.Upsert([]PointStruct{{ID: "2", Vector: []float32{1, 0, 0}}})
	if err == nil {
		t.Fatal("expected a dimension mismatch error")
	}
	if !strings.Contains(err.Error(), "dimension") {
		t.Fatalf("error should mention the dimension, got: %v", err)
	}
	if idx.Count() != 1 {
		t.Fatalf("Count() = %d after a failed upsert, want 1", idx.Count())
	}
	if _, ok := idx.points["2"]; ok {
		t.Fatal("failed upsert leaked point 2 into the points map")
	}
}

// TestCollectionDeleteKeepsBufferOnStorageFailure is a regression test for
// Collection.Delete filtering writeBuf before the storage delete could fail,
// which desynchronized the buffer, the index and the database.
func TestCollectionDeleteKeepsBufferOnStorageFailure(t *testing.T) {
	store := newTempStore(t)

	col, err := store.NewCollection("delete-guard", 2, Cosine, false)
	if err != nil {
		t.Fatalf("new collection: %v", err)
	}
	if err := col.Upsert([]PointStruct{{ID: "1", Vector: []float32{1, 0}}}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	// Make the storage delete fail while leaving the collection usable.
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	if _, err := col.Delete([]string{"1"}, nil); err == nil {
		t.Fatal("expected Delete to fail when storage is closed")
	}

	if len(col.writeBuf) != 1 || col.writeBuf[0].ID != "1" {
		t.Fatalf("write buffer = %+v, want the point to be retained", col.writeBuf)
	}
	if col.Count() != 1 {
		t.Fatalf("index count = %d, want the point to survive a failed delete", col.Count())
	}
}

// TestOpenRejectsNilDatabase is a regression test for Open handing out a store
// whose methods panic on a nil database.
func TestOpenRejectsNilDatabase(t *testing.T) {
	store, err := Open(context.Background(), nil, Config{})
	if err == nil {
		t.Fatalf("Open with a nil database returned %v, want an error", store)
	}
}

// TestStoreMethodsAfterClose ensures closed stores return errors, including a
// zero-value Store, instead of panicking.
func TestStoreMethodsAfterClose(t *testing.T) {
	store := newTempStore(t)
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := store.EnsureCollection("x"); err == nil {
		t.Fatal("expected an error from EnsureCollection after Close")
	}

	var zero Store
	if err := zero.EnsureCollection("x"); err == nil {
		t.Fatal("expected an error from a zero-value Store")
	}
	if err := zero.UpsertPoints("x", nil); err == nil {
		t.Fatal("expected an error from UpsertPoints on a zero-value Store")
	}
	if _, err := zero.LoadCollection("x"); err == nil {
		t.Fatal("expected an error from LoadCollection on a zero-value Store")
	}
	if _, err := zero.ListCollections(); err == nil {
		t.Fatal("expected an error from ListCollections on a zero-value Store")
	}
	if err := zero.DeletePoints("x", nil); err == nil {
		t.Fatal("expected an error from DeletePoints on a zero-value Store")
	}
	if err := zero.DropCollection("x"); err == nil {
		t.Fatal("expected an error from DropCollection on a zero-value Store")
	}
}

// TestStoreCloseIsConcurrencySafe accompanies the atomic closed flag: run with
// -race to catch unsynchronized access.
func TestStoreCloseIsConcurrencySafe(t *testing.T) {
	store := newTempStore(t)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = store.EnsureCollection("concurrent")
			_ = store.Close()
		}()
	}
	wg.Wait()
}

// TestSQ8QuantizerSanitizesNonFinite is a regression test for SQ8 turning
// vectors containing Inf/NaN into all-NaN vectors.
func TestSQ8QuantizerSanitizesNonFinite(t *testing.T) {
	q := NewSQ8Quantizer()

	vectors := [][]float32{
		{1, float32(math.Inf(1)), 2},
		{1, float32(math.Inf(-1)), 2},
		{float32(math.NaN()), float32(math.NaN())},
		{1, 2, 3},
	}

	for _, v := range vectors {
		got := q.Dequantize(q.Quantize(v))
		if len(got) != len(v) {
			t.Fatalf("dequantized vector has length %d, want %d", len(got), len(v))
		}
		for i, f := range got {
			if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
				t.Fatalf("dequantized[%d] of %v = %v, want a finite value", i, v, f)
			}
		}
	}

	// Finite values must still round-trip within the quantization error.
	dequantized := q.Dequantize(q.Quantize([]float32{1, float32(math.Inf(1)), 2}))
	if math.Abs(float64(dequantized[0]-1)) > 0.1 || math.Abs(float64(dequantized[2]-2)) > 0.1 {
		t.Fatalf("finite components were not preserved: %v", dequantized)
	}
}

// TestGetPointsByFilterReturnsCopies is a regression test for the indexes
// returning structs that share their vector slice and payload map with the
// index.
func TestGetPointsByFilterReturnsCopies(t *testing.T) {
	filter := &Filter{Must: []Condition{{Key: "k", Type: MatchTypeExact, Match: MatchValue{Value: "v"}}}}

	check := func(t *testing.T, idx VectorIndex) {
		if err := idx.Upsert([]PointStruct{{
			ID:      "1",
			Vector:  []float32{1, 0},
			Payload: Payload{"k": "v"},
		}}); err != nil {
			t.Fatalf("upsert: %v", err)
		}

		first := idx.GetPointsByFilter(filter)
		if len(first) != 1 {
			t.Fatalf("GetPointsByFilter returned %d points, want 1", len(first))
		}
		first[0].Vector[0] = 99
		first[0].Payload["k"] = "mutated"
		first[0].Payload["extra"] = true

		second := idx.GetPointsByFilter(filter)
		if len(second) != 1 {
			t.Fatalf("GetPointsByFilter returned %d points after mutation, want 1", len(second))
		}
		if second[0].Vector[0] != 1 {
			t.Fatalf("index vector was mutated through the returned point: %v", second[0].Vector)
		}
		if second[0].Payload["k"] != "v" {
			t.Fatalf("index payload was mutated through the returned point: %v", second[0].Payload)
		}
		if _, ok := second[0].Payload["extra"]; ok {
			t.Fatalf("payload map is shared with the index: %v", second[0].Payload)
		}
	}

	t.Run("flat", func(t *testing.T) { check(t, NewFlatIndex(Cosine)) })
	t.Run("hnsw", func(t *testing.T) { check(t, NewHNSWIndex(Cosine)) })
}

// TestDeleteByFilterRejectsNilFilter is a regression test for a nil filter
// wiping the whole index.
func TestDeleteByFilterRejectsNilFilter(t *testing.T) {
	points := []PointStruct{
		{ID: "1", Vector: []float32{1, 0}},
		{ID: "2", Vector: []float32{0, 1}},
	}

	for name, idx := range map[string]VectorIndex{
		"flat": NewFlatIndex(Cosine),
		"hnsw": NewHNSWIndex(Cosine),
	} {
		t.Run(name, func(t *testing.T) {
			if err := idx.Upsert(points); err != nil {
				t.Fatalf("upsert: %v", err)
			}
			if _, err := idx.DeleteByFilter(nil); err == nil {
				t.Fatal("expected DeleteByFilter(nil) to return an error")
			}
			if idx.Count() != len(points) {
				t.Fatalf("Count() = %d after a rejected delete, want %d", idx.Count(), len(points))
			}
		})
	}
}

// TestCalculateDistanceMismatchedLengths is a regression test for the distance
// helpers indexing the shorter slice out of range.
func TestCalculateDistanceMismatchedLengths(t *testing.T) {
	a := []float32{1, 2, 3}
	b := []float32{1, 2}

	if got := CalculateDistance(Euclid, a, b); !math.IsInf(float64(got), 1) {
		t.Fatalf("Euclid distance of mismatched vectors = %v, want +Inf", got)
	}
	if got := CalculateDistance(Cosine, a, b); got != 0 {
		t.Fatalf("cosine similarity of mismatched vectors = %v, want 0", got)
	}
	if got := CalculateDistance(Dot, a, b); got != 0 {
		t.Fatalf("dot product of mismatched vectors = %v, want 0", got)
	}
}

// TestSearchResultsAreOrderedByMetric verifies that both index implementations
// rank their results with their own metric.
func TestSearchResultsAreOrderedByMetric(t *testing.T) {
	points := []PointStruct{
		{ID: "a", Vector: []float32{3, 0}},
		{ID: "b", Vector: []float32{1, 0}},
		{ID: "c", Vector: []float32{2, 0}},
	}

	for name, build := range map[string]func() VectorIndex{
		"flat": func() VectorIndex { return NewFlatIndex(Euclid) },
		"hnsw": func() VectorIndex { return NewHNSWIndex(Euclid) },
	} {
		t.Run(name, func(t *testing.T) {
			idx := build()
			if err := idx.Upsert(points); err != nil {
				t.Fatalf("upsert: %v", err)
			}
			results, err := idx.Search([]float32{0, 0}, nil, 3)
			if err != nil {
				t.Fatalf("search: %v", err)
			}
			want := []string{"b", "c", "a"}
			for i, id := range want {
				if results[i].ID != id {
					t.Fatalf("result #%d = %s, want %s (full: %v)", i, results[i].ID, id, results)
				}
			}
		})
	}
}

// TestStoreWithRootBucketRoundTrip is a smoke test for the root-bucket path,
// which shares the closed/checkOpen plumbing touched above.
func TestStoreWithRootBucketRoundTrip(t *testing.T) {
	store, bdb := openRootTestStore(t, []byte("tenant"))
	_ = bdb

	col, err := store.NewCollection("docs", 2, Cosine, false)
	if err != nil {
		t.Fatalf("new collection: %v", err)
	}
	if err := col.Upsert([]PointStruct{{ID: "1", Vector: []float32{1, 0}, Payload: Payload{"tag": "keep"}}}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	collections, err := store.ListCollections()
	if err != nil {
		t.Fatalf("list collections: %v", err)
	}
	if len(collections) != 1 || collections[0] != "docs" {
		t.Fatalf("collections = %v, want [docs]", collections)
	}
}

// TestStoreCloseIsIdempotent covers the CAS-based closed flag.
func TestStoreCloseIsIdempotent(t *testing.T) {
	store := newTempStore(t)
	if err := store.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
