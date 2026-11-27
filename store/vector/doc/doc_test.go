package doc

import (
	"path/filepath"
	"testing"

	"github.com/malivvan/tempest"
	"github.com/malivvan/tempest/store/vector"
)

// newStore returns a throwaway tempest database's vector store.
func newStore(t *testing.T) *vector.Store {
	t.Helper()
	st, err := tempest.Open(t.Context(), filepath.Join(t.TempDir(), "test.db"), 0o600,
		&tempest.Options{Password: "test-password"})
	if err != nil {
		t.Fatalf("tempest.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st.Vector()
}

func newIndex(t *testing.T, name string) *Index {
	t.Helper()
	idx, err := Open(newStore(t), name)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return idx
}

// unitVec returns a unit vector along axis i of dim dimensions.
func unitVec(dim, i int) []float32 {
	v := make([]float32, dim)
	v[i] = 1
	return v
}

func TestOpenValidation(t *testing.T) {
	if _, err := Open(nil, "x"); err == nil {
		t.Fatal("Open(nil store): want an error")
	}
	if _, err := Open(newStore(t), ""); err == nil {
		t.Fatal("Open with an empty name: want an error")
	}
}

func TestEmptyIndexIsNotAnError(t *testing.T) {
	idx := newIndex(t, "empty")

	if n := idx.Count(); n != 0 {
		t.Errorf("Count on a fresh index = %d, want 0", n)
	}
	res, err := idx.Search(unitVec(4, 0), 5)
	if err != nil {
		t.Fatalf("Search on an empty index must not error: %v", err)
	}
	if len(res) != 0 {
		t.Errorf("Search returned %d results from an empty index", len(res))
	}
	all, err := idx.All()
	if err != nil {
		t.Fatalf("All on an empty index: %v", err)
	}
	if len(all) != 0 {
		t.Errorf("All returned %d documents from an empty index", len(all))
	}
	if err := idx.Flush(); err != nil {
		t.Errorf("Flush on an empty index: %v", err)
	}
	if err := idx.Delete("nope"); err != nil {
		t.Errorf("Delete on an empty index must be a no-op: %v", err)
	}
	if _, ok, err := idx.Get("nope"); err != nil || ok {
		t.Errorf("Get on an empty index = ok=%v err=%v, want ok=false err=nil", ok, err)
	}
}

// TestDimensionComesFromFirstUpsert is the ergonomic point of this package: the
// caller never declares a vector length.
func TestDimensionComesFromFirstUpsert(t *testing.T) {
	idx := newIndex(t, "dim")
	if err := idx.Upsert(&Document{ID: "a", Content: "alpha", Embedding: unitVec(8, 0)}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if n := idx.Count(); n != 1 {
		t.Fatalf("Count = %d, want 1", n)
	}
	// A second document with the same dimension is accepted.
	if err := idx.Upsert(&Document{ID: "b", Content: "beta", Embedding: unitVec(8, 1)}); err != nil {
		t.Fatalf("second Upsert: %v", err)
	}
	// A different dimension must be rejected rather than misinterpreted.
	if err := idx.Upsert(&Document{ID: "c", Content: "gamma", Embedding: unitVec(3, 0)}); err == nil {
		t.Fatal("Upsert with a mismatched dimension: want an error")
	}
}

func TestUpsertGetRoundTrip(t *testing.T) {
	idx := newIndex(t, "roundtrip")
	d := &Document{
		ID:        "doc-1",
		Content:   "the quick brown fox",
		Embedding: unitVec(4, 2),
		Metadata:  map[string]string{"path": "a/b.txt", "kind": "note"},
	}
	if err := idx.Upsert(d); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	got, ok, err := idx.Get("doc-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok {
		t.Fatal("Get reported the document as missing")
	}
	if got.Content != d.Content {
		t.Errorf("content = %q, want %q", got.Content, d.Content)
	}
	if got.Metadata["path"] != "a/b.txt" || got.Metadata["kind"] != "note" {
		t.Errorf("metadata = %v", got.Metadata)
	}
	if len(got.Embedding) != 4 || got.Embedding[2] != 1 {
		t.Errorf("embedding = %v", got.Embedding)
	}

	if _, ok, err := idx.Get("absent"); err != nil || ok {
		t.Errorf("Get(absent) = ok=%v err=%v, want ok=false err=nil", ok, err)
	}
}

func TestUpsertReplacesSameID(t *testing.T) {
	idx := newIndex(t, "replace")
	if err := idx.Upsert(&Document{ID: "x", Content: "first", Embedding: unitVec(4, 0)}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := idx.Upsert(&Document{ID: "x", Content: "second", Embedding: unitVec(4, 0)}); err != nil {
		t.Fatalf("second Upsert: %v", err)
	}
	if n := idx.Count(); n != 1 {
		t.Fatalf("Count = %d, want 1 after replacing the same id", n)
	}
	got, _, _ := idx.Get("x")
	if got.Content != "second" {
		t.Errorf("content = %q, want %q", got.Content, "second")
	}
}

func TestSearchRanksBySimilarity(t *testing.T) {
	idx := newIndex(t, "search")
	if err := idx.Upsert(&Document{ID: "axis0", Content: "cats", Embedding: unitVec(4, 0)}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := idx.Upsert(&Document{ID: "axis1", Content: "dogs", Embedding: unitVec(4, 1)}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	res, err := idx.Search(unitVec(4, 0), 2)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("got %d results, want 2", len(res))
	}
	if res[0].ID != "axis0" {
		t.Errorf("top result = %q, want axis0", res[0].ID)
	}
	if res[0].Score < 0.99 {
		t.Errorf("top score = %f, want ~1.0", res[0].Score)
	}
	// Payload survives the search path, not only Get.
	if res[0].Content != "cats" {
		t.Errorf("search result content = %q, want cats", res[0].Content)
	}
}

func TestDeleteRemovesDocument(t *testing.T) {
	idx := newIndex(t, "delete")
	if err := idx.Upsert(&Document{ID: "gone", Content: "x", Embedding: unitVec(4, 0)}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := idx.Delete("gone"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, _ := idx.Get("gone"); ok {
		t.Fatal("document still present after Delete")
	}
	if err := idx.Delete("gone"); err != nil {
		t.Fatalf("second Delete must be a no-op: %v", err)
	}
}

func TestAllReportsEveryDocument(t *testing.T) {
	idx := newIndex(t, "all")
	want := map[string]bool{"a": true, "b": true, "c": true}
	for id := range want {
		if err := idx.Upsert(&Document{ID: id, Content: id, Embedding: unitVec(4, 0)}); err != nil {
			t.Fatalf("Upsert(%s): %v", id, err)
		}
	}
	all, err := idx.All()
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(all) != len(want) {
		t.Fatalf("All returned %d documents, want %d", len(all), len(want))
	}
	for _, d := range all {
		if !want[d.ID] {
			t.Errorf("unexpected document %q", d.ID)
		}
		if len(d.Embedding) != 4 {
			t.Errorf("document %q has no embedding", d.ID)
		}
	}
}

// TestReopenAfterCloseStillSearches: the collection is reopened from stored
// metadata, so a new process (or a fresh Index over the same database) sees the
// documents and does not need the dimension declared again.
func TestReopenAfterCloseStillSearches(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "reopen.db")

	st1, err := tempest.Open(t.Context(), path, 0o600, &tempest.Options{Password: "pw"})
	if err != nil {
		t.Fatalf("tempest.Open: %v", err)
	}
	idx1, err := Open(st1.Vector(), "persist")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := idx1.Upsert(&Document{ID: "kept", Content: "hello", Embedding: unitVec(4, 1)}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := idx1.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := st1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	st2, err := tempest.Open(t.Context(), path, 0o600, &tempest.Options{Password: "pw"})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })

	idx2, err := Open(st2.Vector(), "persist")
	if err != nil {
		t.Fatalf("Open after reopen: %v", err)
	}
	if n := idx2.Count(); n != 1 {
		t.Fatalf("Count after reopen = %d, want 1", n)
	}
	res, err := idx2.Search(unitVec(4, 1), 1)
	if err != nil {
		t.Fatalf("Search after reopen: %v", err)
	}
	if len(res) != 1 || res[0].ID != "kept" {
		t.Fatalf("Search after reopen = %+v", res)
	}
}

// TestSeparateNamesAreIsolated: two Indexes on one store do not see each other.
func TestSeparateNamesAreIsolated(t *testing.T) {
	st := newStore(t)
	a, err := Open(st, "a")
	if err != nil {
		t.Fatalf("Open a: %v", err)
	}
	b, err := Open(st, "b")
	if err != nil {
		t.Fatalf("Open b: %v", err)
	}
	if err := a.Upsert(&Document{ID: "only-a", Content: "x", Embedding: unitVec(4, 0)}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if n := b.Count(); n != 0 {
		t.Fatalf("index b sees %d documents from index a", n)
	}
	if _, ok, _ := b.Get("only-a"); ok {
		t.Fatal("index b returned a document from index a")
	}
}

func TestUpsertRejectsEmptyID(t *testing.T) {
	idx := newIndex(t, "bad")
	if err := idx.Upsert(&Document{Embedding: unitVec(4, 0)}); err == nil {
		t.Fatal("Upsert without an id: want an error")
	}
	if err := idx.Upsert(nil); err == nil {
		t.Fatal("Upsert(nil): want an error")
	}
}

func TestNilMetadataIsAccepted(t *testing.T) {
	idx := newIndex(t, "nilmeta")
	if err := idx.Upsert(&Document{ID: "n", Content: "c", Embedding: unitVec(4, 0)}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	got, ok, err := idx.Get("n")
	if err != nil || !ok {
		t.Fatalf("Get = ok=%v err=%v", ok, err)
	}
	if got.Metadata == nil {
		t.Error("Metadata is nil; it should be an empty map")
	}
}
