package search

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/malivvan/tempest/internal/bucket"
	bolt "go.etcd.io/bbolt"
)

func testStore(t *testing.T, cfg Config) (*Store, *bolt.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fts.db")
	bdb, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	t.Cleanup(func() { _ = bdb.Close() })
	cfg.Root = [][]byte{[]byte(bucket.SEARCH)}
	s, err := Open(context.Background(), bdb, cfg)
	if err != nil {
		t.Fatalf("fts.Open: %v", err)
	}
	return s, bdb
}

func openIndex(t *testing.T, s *Store) *Index {
	t.Helper()
	idx, err := s.Open("docs", Config{})
	if err != nil {
		t.Fatalf("Open index: %v", err)
	}
	return idx
}

func TestWordsTokenizer(t *testing.T) {
	got := Words().Tokenize("Hello, WORLD! 42 café — 世界")
	want := []string{"hello", "world", "42", "café", "世界"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("Words = %v, want %v", got, want)
	}
}

func TestWordsWithCJKTokenizer(t *testing.T) {
	got := WordsWithCJK().Tokenize("東京タワー")
	// Words emits the whole run; trigrams cover it as well.
	has := func(want string) bool {
		for _, term := range got {
			if term == want {
				return true
			}
		}
		return false
	}
	for _, want := range []string{"東京タワー", "東京タ", "京タワ", "タワー"} {
		if !has(want) {
			t.Errorf("WordsWithCJK missing %q in %v", want, got)
		}
	}

	// A short CJK run contributes itself.
	short := WordsWithCJK().Tokenize("日本")
	if !containsTerm(short, "日本") {
		t.Fatalf("short CJK run not emitted: %v", short)
	}
}

func containsTerm(terms []string, want string) bool {
	for _, term := range terms {
		if term == want {
			return true
		}
	}
	return false
}

func TestIndexUpsertSearchDelete(t *testing.T) {
	ctx := context.Background()
	s, _ := testStore(t, Config{})
	idx := openIndex(t, s)

	if err := idx.Upsert(ctx, "a", "the quick brown fox"); err != nil {
		t.Fatalf("Upsert a: %v", err)
	}
	if err := idx.Upsert(ctx, "b", "the lazy dog"); err != nil {
		t.Fatalf("Upsert b: %v", err)
	}
	if err := idx.Upsert(ctx, "c", "foxes and foxes"); err != nil {
		t.Fatalf("Upsert c: %v", err)
	}

	hits, err := idx.Search(ctx, "fox", 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 1 || hits[0].ID != "a" {
		t.Fatalf("Search(fox) = %+v, want just a", hits)
	}

	// Repeated terms rank higher than one occurrence in equal-length docs.
	if err := idx.Upsert(ctx, "d", "zx qq"); err != nil {
		t.Fatal(err)
	}
	if err := idx.Upsert(ctx, "e", "zx zx"); err != nil {
		t.Fatal(err)
	}
	hits, err = idx.Search(ctx, "zx", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 || hits[0].ID != "e" {
		t.Fatalf("Search(zx) = %+v, want e first", hits)
	}

	// Deterministic tie ordering by id.
	if err := idx.Upsert(ctx, "f", "tie term"); err != nil {
		t.Fatal(err)
	}
	if err := idx.Upsert(ctx, "g", "tie term"); err != nil {
		t.Fatal(err)
	}
	hits, err = idx.Search(ctx, "tie", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 || hits[0].ID != "f" || hits[1].ID != "g" {
		t.Fatalf("tie order = %+v, want f then g", hits)
	}

	count, err := idx.Count(ctx)
	if err != nil || count != 7 {
		t.Fatalf("Count = %d err=%v, want 7", count, err)
	}

	// Delete removes postings and statistics.
	if err := idx.Delete(ctx, "a", "c", "missing"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	hits, err = idx.Search(ctx, "fox", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 0 {
		t.Fatalf("Search(fox) after delete = %+v", hits)
	}
	count, err = idx.Count(ctx)
	if err != nil || count != 5 {
		t.Fatalf("Count after delete = %d err=%v, want 5", count, err)
	}
}

func TestIndexReindexReplacesPostings(t *testing.T) {
	ctx := context.Background()
	s, _ := testStore(t, Config{})
	idx := openIndex(t, s)

	if err := idx.Upsert(ctx, "doc", "alpha beta"); err != nil {
		t.Fatal(err)
	}
	if err := idx.Upsert(ctx, "doc", "gamma"); err != nil {
		t.Fatal(err)
	}

	if hits, _ := idx.Search(ctx, "alpha", 10); len(hits) != 0 {
		t.Fatalf("old term still matches: %+v", hits)
	}
	hits, err := idx.Search(ctx, "gamma", 10)
	if err != nil || len(hits) != 1 || hits[0].ID != "doc" {
		t.Fatalf("Search(gamma) = %+v err=%v", hits, err)
	}
	if count, _ := idx.Count(ctx); count != 1 {
		t.Fatalf("Count = %d, want 1 after re-index", count)
	}
}

func TestIndexPersistenceAcrossReopen(t *testing.T) {
	ctx := context.Background()
	s, bdb := testStore(t, Config{Tokenizer: WordsWithCJK()})
	idx, err := s.Open("docs", Config{Tokenizer: WordsWithCJK()})
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.Upsert(ctx, "jp", "東京タワー"); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(ctx, bdb, Config{Root: [][]byte{[]byte(bucket.SEARCH)}})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	idx2, err := reopened.Open("docs", Config{Tokenizer: WordsWithCJK()})
	if err != nil {
		t.Fatalf("reopen index: %v", err)
	}
	hits, err := idx2.Search(ctx, "東京タ", 10)
	if err != nil {
		t.Fatalf("search after reopen: %v", err)
	}
	if len(hits) != 1 || hits[0].ID != "jp" {
		t.Fatalf("search after reopen = %+v", hits)
	}

	// The tokenizer is part of the index configuration.
	if _, err := reopened.Open("docs", Config{Tokenizer: Words()}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Open with a different tokenizer: err = %v, want ErrInvalidConfig", err)
	}
}

func TestIndexEmptyQueriesAndLimits(t *testing.T) {
	ctx := context.Background()
	s, _ := testStore(t, Config{})
	idx := openIndex(t, s)

	if hits, err := idx.Search(ctx, "", 10); err != nil || len(hits) != 0 {
		t.Fatalf("empty query = %+v err=%v", hits, err)
	}
	if hits, err := idx.Search(ctx, "nothing", 10); err != nil || len(hits) != 0 {
		t.Fatalf("missing term = %+v err=%v", hits, err)
	}

	if err := idx.Upsert(ctx, "a", "common term one"); err != nil {
		t.Fatal(err)
	}
	if err := idx.Upsert(ctx, "b", "common term two"); err != nil {
		t.Fatal(err)
	}
	hits, err := idx.Search(ctx, "common", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 {
		t.Fatalf("limit 1 returned %d hits", len(hits))
	}
	all, err := idx.Search(ctx, "common", 0)
	if err != nil || len(all) != 2 {
		t.Fatalf("limit 0 = %+v err=%v, want both", all, err)
	}
}

func TestIndexEncryptedValuesAndTokens(t *testing.T) {
	ctx := context.Background()
	secret := []byte("0123456789abcdef0123456789abcdef")
	s, bdb := testStore(t, Config{Secret: secret, EncryptValues: true, EncryptTokens: true})
	idx := openIndex(t, s)
	if err := idx.Upsert(ctx, "secret-doc", "classified material"); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	plain, err := Open(ctx, bdb, Config{Root: [][]byte{[]byte(bucket.SEARCH)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plain.Open("docs", Config{}); !errors.Is(err, ErrWrongMode) {
		t.Fatalf("opening an encrypted index without encryption: err = %v, want ErrWrongMode", err)
	}

	again, err := Open(ctx, bdb, Config{Root: [][]byte{[]byte(bucket.SEARCH)}, Secret: secret, EncryptValues: true, EncryptTokens: true})
	if err != nil {
		t.Fatal(err)
	}
	idx2, err := again.Open("docs", Config{})
	if err != nil {
		t.Fatal(err)
	}
	hits, err := idx2.Search(ctx, "classified", 10)
	if err != nil {
		t.Fatalf("search encrypted: %v", err)
	}
	if len(hits) != 1 || hits[0].ID != "secret-doc" {
		t.Fatalf("encrypted search = %+v", hits)
	}
}

func TestIndexRejectsWrongVersion(t *testing.T) {
	s, _ := testStore(t, Config{})
	_ = openIndex(t, s)

	// Corrupt the version record.
	if err := s.update(func(tx *bolt.Tx) error {
		b, err := s.indexBucket(tx, "docs")
		if err != nil {
			return err
		}
		return b.Put([]byte(versionKey), []byte{9, 0, 0, 0, 0, 0, 0, 0})
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open("docs", Config{}); !errors.Is(err, ErrIncompatibleVersion) {
		t.Fatalf("Open after version bump: err = %v, want ErrIncompatibleVersion", err)
	}
}

func TestIndexesListsNames(t *testing.T) {
	ctx := context.Background()
	s, _ := testStore(t, Config{})
	if _, err := s.Open("one", Config{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open("two", Config{}); err != nil {
		t.Fatal(err)
	}
	names, err := s.Indexes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(names, ",") != "one,two" {
		t.Fatalf("Indexes = %v, want [one two]", names)
	}
}

func TestIndexMissingName(t *testing.T) {
	s, _ := testStore(t, Config{})
	if _, err := s.Open("", Config{}); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("Open(\"\") err = %v, want ErrInvalidName", err)
	}
	if _, err := s.Open("a/b", Config{}); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("Open(a/b) err = %v, want ErrInvalidName", err)
	}
}
