package vec

import (
	"math"
	"testing"
)

func TestSearchWithVectors(t *testing.T) {
	for _, useHNSW := range []bool{false, true} {
		store := newTempStore(t)
		col, err := store.NewCollection("docs", 2, Cosine, useHNSW)
		if err != nil {
			t.Fatal(err)
		}
		if err := col.Upsert([]PointStruct{
			{ID: "a", Vector: []float32{1, 0}},
			{ID: "b", Vector: []float32{0, 1}},
		}); err != nil {
			t.Fatal(err)
		}

		plain, err := col.SearchWith([]float32{1, 0}, nil, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, hit := range plain {
			if hit.Vector != nil {
				t.Fatalf("default search returned a vector: %+v", hit)
			}
		}

		withVectors, err := col.SearchWith([]float32{1, 0}, nil, 2, WithVectors())
		if err != nil {
			t.Fatal(err)
		}
		if len(withVectors) != 2 {
			t.Fatalf("got %d hits", len(withVectors))
		}
		for _, hit := range withVectors {
			switch hit.ID {
			case "a":
				if len(hit.Vector) != 2 || hit.Vector[0] != 1 || hit.Vector[1] != 0 {
					t.Fatalf("a vector = %v", hit.Vector)
				}
			case "b":
				if len(hit.Vector) != 2 || hit.Vector[0] != 0 || hit.Vector[1] != 1 {
					t.Fatalf("b vector = %v", hit.Vector)
				}
			}
		}

		// The returned vectors are copies, not the index's storage.
		withVectors[0].Vector[0] = 99
		point, err := col.Get("a")
		if err != nil {
			t.Fatal(err)
		}
		if point.Vector[0] == 99 {
			t.Fatal("mutating a result vector changed the stored point")
		}
	}
}

func TestSearchWithScorer(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := col.Upsert([]PointStruct{
		{ID: "a", Vector: []float32{1, 0}},
		{ID: "b", Vector: []float32{1, 1}},
	}); err != nil {
		t.Fatal(err)
	}

	// A scorer that prefers the second element flips the cosine ordering.
	score := func(_, vector []float32) float64 { return float64(vector[1]) }
	hits, err := col.SearchWith([]float32{1, 0}, nil, 2, WithScorer(score))
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 || hits[0].ID != "b" || hits[0].Score != 1 {
		t.Fatalf("scored hits = %+v", hits)
	}

	// Filters still select the candidates before scoring.
	filter := &Filter{Must: []Condition{{Key: "none", Match: MatchValue{Value: true}}}}
	if err := col.Upsert([]PointStruct{{ID: "c", Vector: []float32{0, 1}, Payload: Payload{"none": true}}}); err != nil {
		t.Fatal(err)
	}
	hits, err = col.SearchWith([]float32{1, 0}, filter, 5, WithScorer(score))
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].ID != "c" {
		t.Fatalf("filtered scored hits = %+v", hits)
	}
}

func TestSearchWithScorerTieOrdering(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := col.Upsert([]PointStruct{
		{ID: "b", Vector: []float32{1, 0}},
		{ID: "a", Vector: []float32{1, 0}},
	}); err != nil {
		t.Fatal(err)
	}
	hits, err := col.SearchWith([]float32{1, 0}, nil, 2, WithScorer(func(_, _ []float32) float64 {
		return math.Round(0)
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 || hits[0].ID != "a" || hits[1].ID != "b" {
		t.Fatalf("tie order = %+v", hits)
	}
}

func TestSearchWithValidation(t *testing.T) {
	store := newTempStore(t)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.SearchWith([]float32{1, 0, 0}, nil, 1); err == nil {
		t.Fatal("dimension mismatch: want an error")
	}
	if _, err := col.SearchWith([]float32{1, 0}, nil, 0); err == nil {
		t.Fatal("topK 0: want an error")
	}

	// Options that change nothing fall back to the plain path.
	if err := col.Upsert([]PointStruct{{ID: "a", Vector: []float32{1, 0}}}); err != nil {
		t.Fatal(err)
	}
	hits, err := col.SearchWith([]float32{1, 0}, nil, 1, WithScorer(nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].ID != "a" {
		t.Fatalf("hits = %+v", hits)
	}
}
