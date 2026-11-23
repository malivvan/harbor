package vector

import (
	"errors"
	"math"
	"sort"
	"testing"
)

// seedRange writes a small point set used by the range-search tests.
func seedRange(t *testing.T, c *Collection) {
	t.Helper()
	err := c.Upsert([]PointStruct{
		{ID: "east", Vector: []float32{1, 0}, Payload: Payload{"kind": "a"}},
		{ID: "north", Vector: []float32{0, 1}, Payload: Payload{"kind": "b"}},
		{ID: "diag", Vector: []float32{1, 1}, Payload: Payload{"kind": "a"}},
	})
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
}

func rangeIDs(hits []ScoredPoint) []string {
	out := make([]string, len(hits))
	for i, h := range hits {
		out[i] = h.ID
	}
	return out
}

func TestRangeSearchCosine(t *testing.T) {
	for _, useHNSW := range []bool{false, true} {
		name := "flat"
		if useHNSW {
			name = "hnsw"
		}
		t.Run(name, func(t *testing.T) {
			store := newTempStore(t)
			col, err := store.NewCollection("docs", 2, Cosine, useHNSW)
			if err != nil {
				t.Fatal(err)
			}
			seedRange(t, col)

			// 1 - cos(45 degrees) ≈ 0.2929 for the diagonal point, so a radius
			// of 0.3 includes it and a radius of 0.1 does not.
			near, err := col.RangeSearch([]float32{1, 0}, 0.1, nil)
			if err != nil {
				t.Fatalf("RangeSearch: %v", err)
			}
			if got := rangeIDs(near); len(got) != 1 || got[0] != "east" {
				t.Fatalf("radius 0.1 = %v, want [east]", got)
			}
			if near[0].Score != 1 {
				t.Fatalf("east score = %v, want 1", near[0].Score)
			}

			wide, err := col.RangeSearch([]float32{1, 0}, 0.3, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got := rangeIDs(wide); len(got) != 2 || got[0] != "east" || got[1] != "diag" {
				t.Fatalf("radius 0.3 = %v, want [east diag] (best first)", got)
			}
			// Scores are cosine similarities, descending.
			if wide[0].Score <= wide[1].Score {
				t.Fatalf("scores not descending: %v", wide)
			}
		})
	}
}

func TestRangeSearchEuclid(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Euclid, false)
	if err != nil {
		t.Fatal(err)
	}
	seedRange(t, col)

	hits, err := col.RangeSearch([]float32{1, 0}, 1.0, nil)
	if err != nil {
		t.Fatal(err)
	}
	// east distance 0, north distance sqrt(2)≈1.414, diag distance 1.
	if got := rangeIDs(hits); len(got) != 2 || got[0] != "east" || got[1] != "diag" {
		t.Fatalf("RangeSearch = %v, want [east diag]", got)
	}
	if math.Abs(float64(hits[1].Score)-1) > 1e-6 {
		t.Fatalf("diag score = %v, want 1", hits[1].Score)
	}

	// Zero radius keeps only an exact match.
	zero, err := col.RangeSearch([]float32{1, 0}, 0, nil)
	if err != nil || len(zero) != 1 || zero[0].ID != "east" {
		t.Fatalf("radius 0 = %v err=%v", rangeIDs(zero), err)
	}
}

func TestRangeSearchDot(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Dot, false)
	if err != nil {
		t.Fatal(err)
	}
	seedRange(t, col)
	if err := col.Upsert([]PointStruct{{ID: "west", Vector: []float32{-1, 0}}}); err != nil {
		t.Fatal(err)
	}

	// dot(query=[1,0], east)=1 and dot(diag=[1,1])=1 → distance -1;
	// dot(north=[0,1])=0 → distance 0; dot(west=[-1,0])=-1 → distance 1.
	hits, err := col.RangeSearch([]float32{1, 0}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := rangeIDs(hits); len(got) != 3 || got[0] != "diag" || got[1] != "east" || got[2] != "north" {
		t.Fatalf("RangeSearch = %v, want [diag east north]", got)
	}
	if hits[0].Score != 1 || hits[1].Score != 1 || hits[2].Score != 0 {
		t.Fatalf("scores = %v, want 1, 1, 0", hits)
	}

	// A point with a negative dot product is farther than the radius.
	for _, hit := range hits {
		if hit.ID == "west" {
			t.Fatal("west is farther than radius 0")
		}
	}
}

func TestRangeSearchFilterAndLimit(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	if err != nil {
		t.Fatal(err)
	}
	seedRange(t, col)

	filter := &Filter{Must: []Condition{{Key: "kind", Match: MatchValue{Value: "a"}}}}
	hits, err := col.RangeSearch([]float32{1, 0}, 1.0, filter)
	if err != nil {
		t.Fatal(err)
	}
	if got := rangeIDs(hits); len(got) != 2 || got[0] != "east" || got[1] != "diag" {
		t.Fatalf("filtered = %v", got)
	}

	limited, err := col.RangeSearchN([]float32{1, 0}, 1.0, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 1 || limited[0].ID != "east" {
		t.Fatalf("limited = %v", rangeIDs(limited))
	}

	none, err := col.RangeSearchN([]float32{1, 0}, 1.0, filter, 0)
	if err != nil || len(none) != 2 {
		t.Fatalf("limit 0 = %v err=%v", rangeIDs(none), err)
	}
}

func TestRangeSearchValidation(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	if err != nil {
		t.Fatal(err)
	}
	seedRange(t, col)

	if _, err := col.RangeSearch([]float32{1, 0, 0}, 0.5, nil); err == nil {
		t.Fatal("dimension mismatch: want an error")
	}
	if _, err := col.RangeSearch([]float32{1, 0}, -0.1, nil); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("negative radius err = %v, want ErrInvalidOptions", err)
	}
	if _, err := col.RangeSearchN([]float32{1, 0}, 0.5, nil, -1); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("negative limit err = %v, want ErrInvalidOptions", err)
	}

	empty, err := col.RangeSearch([]float32{-1, 0}, 0.1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("empty result = %v", rangeIDs(empty))
	}
}

// TestRangeSearchMatchesSearchScores proves a radius query returns the same
// score the top-K search reports for the same point.
func TestRangeSearchMatchesSearchScores(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	if err != nil {
		t.Fatal(err)
	}
	seedRange(t, col)

	top, err := col.Search([]float32{1, 0}, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float32{}
	for _, hit := range top {
		want[hit.ID] = hit.Score
	}

	rangeHits, err := col.RangeSearch([]float32{1, 0}, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rangeHits) != len(top) {
		t.Fatalf("range hits = %d, top hits = %d", len(rangeHits), len(top))
	}
	for _, hit := range rangeHits {
		if want[hit.ID] != hit.Score {
			t.Fatalf("%s score %v, Search reports %v", hit.ID, hit.Score, want[hit.ID])
		}
	}
}

// TestRangeSearchSortedByIDOnTies makes the ordering deterministic.
func TestRangeSearchSortedByIDOnTies(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := col.Upsert([]PointStruct{
		{ID: "b", Vector: []float32{1, 0}},
		{ID: "a", Vector: []float32{1, 0}},
		{ID: "c", Vector: []float32{1, 0}},
	}); err != nil {
		t.Fatal(err)
	}
	hits, err := col.RangeSearch([]float32{1, 0}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	ids := rangeIDs(hits)
	if !sort.StringsAreSorted(ids) {
		t.Fatalf("tie order = %v, want sorted ids", ids)
	}
}
