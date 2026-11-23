package vec

import (
	"errors"
	"sync"
	"testing"
)

func seedFacets(t *testing.T, c *Collection) {
	t.Helper()
	err := c.Upsert([]PointStruct{
		{ID: "1", Vector: []float32{1, 0}, Payload: Payload{"kind": "a", "lang": "en", "score": 1}},
		{ID: "2", Vector: []float32{1, 0}, Payload: Payload{"kind": "a", "lang": "fr"}},
		{ID: "3", Vector: []float32{1, 0}, Payload: Payload{"kind": "b", "lang": "en"}},
		{ID: "4", Vector: []float32{1, 0}, Payload: Payload{"kind": "c"}},
		{ID: "5", Vector: []float32{1, 0}, Payload: Payload{"lang": "en"}},
	})
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
}

func facetByField(results []FacetResult, field string) *FacetResult {
	for i := range results {
		if results[i].Field == field {
			return &results[i]
		}
	}
	return nil
}

func TestFacetCounts(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	if err != nil {
		t.Fatal(err)
	}
	seedFacets(t, col)

	results, err := col.FacetCounts(FacetRequest{Fields: []string{"kind", "lang", "missing"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}
	for _, result := range results {
		if result.Total != 5 {
			t.Fatalf("%s Total = %d, want 5", result.Field, result.Total)
		}
	}

	kind := facetByField(results, "kind")
	if kind == nil || kind.Values["a"] != 2 || kind.Values["b"] != 1 || kind.Values["c"] != 1 {
		t.Fatalf("kind = %+v", kind)
	}
	lang := facetByField(results, "lang")
	if lang == nil || lang.Values["en"] != 3 || lang.Values["fr"] != 1 {
		t.Fatalf("lang = %+v", lang)
	}
	// A field missing everywhere yields an empty map, not an error.
	if missing := facetByField(results, "missing"); missing == nil || len(missing.Values) != 0 {
		t.Fatalf("missing = %+v", missing)
	}
	// Numeric values are skipped.
	if kind.Values["1"] != 0 {
		t.Fatalf("numeric payload value was counted: %+v", kind.Values)
	}
}

func TestFacetCountsFilterScoping(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	if err != nil {
		t.Fatal(err)
	}
	seedFacets(t, col)

	filter := &Filter{Must: []Condition{{Key: "lang", Match: MatchValue{Value: "en"}}}}
	results, err := col.FacetCounts(FacetRequest{Fields: []string{"kind"}, Filter: filter})
	if err != nil {
		t.Fatal(err)
	}
	kind := facetByField(results, "kind")
	if kind == nil {
		t.Fatal("no kind result")
	}
	if kind.Total != 3 {
		t.Fatalf("Total = %d, want 3", kind.Total)
	}
	if kind.Values["a"] != 1 || kind.Values["b"] != 1 {
		t.Fatalf("kind = %+v", kind.Values)
	}
}

func TestFacetCountsMaxValues(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	if err != nil {
		t.Fatal(err)
	}
	seedFacets(t, col)

	results, err := col.FacetCounts(FacetRequest{Fields: []string{"kind"}, MaxValues: 1})
	if err != nil {
		t.Fatal(err)
	}
	kind := facetByField(results, "kind")
	if len(kind.Values) != 1 || kind.Values["a"] != 2 {
		t.Fatalf("top value = %+v, want a:2", kind.Values)
	}
	if kind.Total != 5 {
		t.Fatalf("Total = %d, want 5 before truncation", kind.Total)
	}
}

func TestFacetCountsEmptyAndValidation(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	if err != nil {
		t.Fatal(err)
	}

	empty, err := col.FacetCounts(FacetRequest{Fields: []string{"kind"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 1 || empty[0].Total != 0 || len(empty[0].Values) != 0 {
		t.Fatalf("empty collection facets = %+v", empty)
	}

	none, err := col.FacetCounts(FacetRequest{})
	if err != nil || len(none) != 0 {
		t.Fatalf("no fields = %+v err=%v", none, err)
	}

	if _, err := col.FacetCounts(FacetRequest{Fields: []string{"kind"}, MaxValues: -1}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("negative MaxValues err = %v, want ErrInvalidOptions", err)
	}
	if _, err := col.FacetCounts(FacetRequest{Fields: []string{""}}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("empty field err = %v, want ErrInvalidOptions", err)
	}
}

func TestFacetCountsDuplicateFields(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	if err != nil {
		t.Fatal(err)
	}
	seedFacets(t, col)

	results, err := col.FacetCounts(FacetRequest{Fields: []string{"kind", "kind"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("duplicate field produced %d results", len(results))
	}
}

func TestFacetCountsConcurrentWrites(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	if err != nil {
		t.Fatal(err)
	}
	seedFacets(t, col)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				_ = col.Upsert([]PointStruct{{
					ID:      "hot-" + string(rune('a'+i)) + string(rune('a'+j%26)),
					Vector:  []float32{1, 0},
					Payload: Payload{"kind": "a"},
				}})
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 25; j++ {
			if _, err := col.FacetCounts(FacetRequest{Fields: []string{"kind"}}); err != nil {
				t.Errorf("FacetCounts: %v", err)
			}
		}
	}()
	wg.Wait()
}
