package ranking

import (
	"math"
	"testing"
)

func TestRRFMergesLists(t *testing.T) {
	text := []Result{{ID: "a", Score: 9}, {ID: "b", Score: 5}}
	vector := []Result{{ID: "b", Score: 0.9}, {ID: "c", Score: 0.8}}

	got := RRF(60, text, vector)
	if len(got) != 3 {
		t.Fatalf("got %d results, want 3", len(got))
	}
	// b appears at rank 2 and rank 1; a and c appear once at rank 1 and 2.
	want := []string{"b", "a", "c"}
	for i, id := range want {
		if got[i].ID != id {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
	if math.Abs(got[0].Score-(1.0/62+1.0/61)) > 1e-12 {
		t.Fatalf("b score = %v", got[0].Score)
	}
}

func TestRRFKAndEmptyLists(t *testing.T) {
	// k <= 0 uses the default.
	defaultFused := RRF(0, []Result{{ID: "a"}})
	explicit := RRF(DefaultRRFK, []Result{{ID: "a"}})
	if len(defaultFused) != 1 || defaultFused[0].Score != explicit[0].Score {
		t.Fatalf("default k mismatch: %v vs %v", defaultFused, explicit)
	}

	if got := RRF(60); len(got) != 0 {
		t.Fatalf("no lists = %v, want empty", got)
	}
	if got := RRF(60, nil, []Result{}); len(got) != 0 {
		t.Fatalf("empty lists = %v, want empty", got)
	}
}

func TestRRFDeterministicTiesAndDuplicates(t *testing.T) {
	list := []Result{{ID: "b"}, {ID: "a"}, {ID: "a"}}
	got := RRF(60, list)
	if len(got) != 2 || got[0].ID != "b" || got[1].ID != "a" {
		t.Fatalf("got %v, want [b a]", got)
	}
	// The duplicate 'a' must contribute at rank 2 only, not twice.
	if math.Abs(got[1].Score-1.0/62) > 1e-12 {
		t.Fatalf("a score = %v, want 1/62", got[1].Score)
	}
}
