package search

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/malivvan/tempest/store/ledger"
)

func restoreSecret() []byte { return bytes.Repeat([]byte{9}, 32) }

func TestSearchMetaDocsTerms(t *testing.T) {
	ctx := context.Background()
	s, _ := testStore(t, Config{})
	idx := openIndex(t, s)

	if err := idx.Upsert(ctx, "d1", "alpha beta beta"); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := idx.Upsert(ctx, "d2", "beta gamma"); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	meta, err := idx.Meta(ctx)
	if err != nil {
		t.Fatalf("Meta: %v", err)
	}
	if meta.DocCount != 2 || meta.TotalLen != 5 || meta.K1 != DefaultK1 || meta.B != DefaultB || meta.Tokenizer != "words" {
		t.Fatalf("Meta = %+v", meta)
	}

	docs, err := idx.Docs(ctx)
	if err != nil {
		t.Fatalf("Docs: %v", err)
	}
	if len(docs) != 2 || docs[0].ID != "d1" || docs[1].ID != "d2" {
		t.Fatalf("Docs = %+v", docs)
	}
	if docs[0].Length != 3 || docs[0].TF["beta"] != 2 || docs[0].TF["alpha"] != 1 {
		t.Fatalf("d1 = %+v", docs[0])
	}

	terms, err := idx.Terms(ctx)
	if err != nil {
		t.Fatalf("Terms: %v", err)
	}
	if len(terms) != 3 || terms[0].Term != "alpha" || terms[1].Term != "beta" {
		t.Fatalf("Terms = %+v", terms)
	}
	if strings.Join(terms[1].Docs, ",") != "d1,d2" {
		t.Fatalf("beta postings = %v", terms[1].Docs)
	}
}

func TestSearchRestoreIndex(t *testing.T) {
	ctx := context.Background()
	src, _ := testStore(t, Config{})
	idx := openIndex(t, src)
	for id, text := range map[string]string{
		"d1": "alpha beta",
		"d2": "beta gamma",
	} {
		if err := idx.Upsert(ctx, id, text); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
	}
	meta, err := idx.Meta(ctx)
	if err != nil {
		t.Fatalf("Meta: %v", err)
	}
	docs, err := idx.Docs(ctx)
	if err != nil {
		t.Fatalf("Docs: %v", err)
	}
	terms, err := idx.Terms(ctx)
	if err != nil {
		t.Fatalf("Terms: %v", err)
	}

	// Restore into a store with value and token encryption.
	dst, _ := testStore(t, Config{Secret: restoreSecret(), EncryptValues: true, EncryptTokens: true})
	if err := dst.RestoreIndex(ctx, "docs", Config{}, meta, docs, terms); err != nil {
		t.Fatalf("RestoreIndex: %v", err)
	}

	got, err := dst.Open("docs", Config{})
	if err != nil {
		t.Fatalf("Open restored index: %v", err)
	}
	count, err := got.Count(ctx)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 2 {
		t.Fatalf("Count = %d, want 2", count)
	}
	hits, err := got.Search(ctx, "beta", 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("Search(beta) = %+v, want both documents", hits)
	}
	gotMeta, err := got.Meta(ctx)
	if err != nil {
		t.Fatalf("restored Meta: %v", err)
	}
	if gotMeta != meta {
		t.Fatalf("restored meta = %+v, want %+v", gotMeta, meta)
	}

	// Restoring replaces the previous contents.
	if err := dst.RestoreIndex(ctx, "docs", Config{}, IndexMeta{K1: DefaultK1, B: DefaultB,
		Tokenizer: "words", DocCount: 1, TotalLen: 1},
		[]DocEntry{{ID: "only", Length: 1, TF: map[string]int{"zulu": 1}}},
		[]TermEntry{{Term: "zulu", Docs: []string{"only"}}}); err != nil {
		t.Fatalf("RestoreIndex replace: %v", err)
	}
	replaced, err := dst.Open("docs", Config{})
	if err != nil {
		t.Fatalf("Open replaced index: %v", err)
	}
	if n, _ := replaced.Count(ctx); n != 1 {
		t.Fatalf("Count after replace = %d, want 1", n)
	}
}

type customRestoreTokenizer struct{}

func (customRestoreTokenizer) Tokenize(text string) []string { return strings.Fields(text) }

func TestSearchRestoreIndexCustomTokenizer(t *testing.T) {
	ctx := context.Background()
	s, _ := testStore(t, Config{})
	meta := IndexMeta{K1: DefaultK1, B: DefaultB, Tokenizer: "custom", DocCount: 1, TotalLen: 1}
	docs := []DocEntry{{ID: "d", Length: 1, TF: map[string]int{"x": 1}}}

	if err := s.RestoreIndex(ctx, "custom", Config{}, meta, docs, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("RestoreIndex without tokenizer = %v, want ErrInvalidConfig", err)
	}

	cfg := Config{Tokenizer: customRestoreTokenizer{}}
	if err := s.RestoreIndex(ctx, "custom", cfg, meta, docs, []TermEntry{{Term: "x", Docs: []string{"d"}}}); err != nil {
		t.Fatalf("RestoreIndex with tokenizer: %v", err)
	}
	idx, err := s.Open("custom", cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if n, _ := idx.Count(ctx); n != 1 {
		t.Fatalf("Count = %d, want 1", n)
	}

	// A custom tokenizer cannot silently stand in for a built-in one.
	builtin := IndexMeta{K1: DefaultK1, B: DefaultB, Tokenizer: "words"}
	if err := s.RestoreIndex(ctx, "builtin", cfg, builtin, docs, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("RestoreIndex builtin with custom tokenizer = %v, want ErrInvalidConfig", err)
	}
}

func TestSearchDrop(t *testing.T) {
	ctx := context.Background()
	s, _ := testStore(t, Config{})
	idx := openIndex(t, s)
	if err := idx.Upsert(ctx, "d1", "alpha"); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	if err := s.Drop(ctx, "docs"); err != nil {
		t.Fatalf("Drop: %v", err)
	}
	names, err := s.Indexes(ctx)
	if err != nil {
		t.Fatalf("Indexes: %v", err)
	}
	if len(names) != 0 {
		t.Fatalf("Indexes after drop = %v", names)
	}

	// Dropping a missing index is a no-op.
	if err := s.Drop(ctx, "docs"); err != nil {
		t.Fatalf("Drop missing: %v", err)
	}
}

// recordingSink collects the audit entries a store reports.
type recordingSink struct {
	entries []ledger.Entry
}

func (r *recordingSink) Record(e ledger.Entry) error {
	r.entries = append(r.entries, e)
	return nil
}

func TestSearchAudit(t *testing.T) {
	ctx := context.Background()
	sink := &recordingSink{}
	s, _ := testStore(t, Config{Audit: sink})
	idx := openIndex(t, s)

	if err := idx.Upsert(ctx, "d1", "alpha beta"); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := idx.Delete(ctx, "d1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Drop(ctx, "docs"); err != nil {
		t.Fatalf("Drop: %v", err)
	}

	wantOps := []string{"create_index", "upsert", "delete", "drop_index"}
	if len(sink.entries) != len(wantOps) {
		t.Fatalf("audit entries = %d, want %d: %+v", len(sink.entries), len(wantOps), sink.entries)
	}
	for i, entry := range sink.entries {
		if entry.Store != ledger.StoreSearch {
			t.Fatalf("entry %d store = %q, want %q", i, entry.Store, ledger.StoreSearch)
		}
		if entry.Op != wantOps[i] {
			t.Fatalf("entry %d op = %q, want %q", i, entry.Op, wantOps[i])
		}
	}
	if sink.entries[1].Entity != "docs:d1" {
		t.Fatalf("upsert entity = %q", sink.entries[1].Entity)
	}
	if len(sink.entries[1].Digest) != 32 {
		t.Fatalf("upsert digest = %d bytes, want 32", len(sink.entries[1].Digest))
	}
	if sink.entries[2].Digest != nil {
		t.Fatalf("delete digest = %x, want nil", sink.entries[2].Digest)
	}
}
