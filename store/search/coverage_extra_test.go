package search

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/malivvan/tempest/internal/bucket"
	bolt "go.etcd.io/bbolt"
)

// newRawBolt opens a fresh bbolt database in a temp dir for tests that need a
// custom Root than testStore provides.
func newRawBolt(t *testing.T) (*bolt.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "extra.db")
	bdb, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	t.Cleanup(func() { _ = bdb.Close() })
	return bdb, path
}

func TestStoreOpenErrors(t *testing.T) {
	ctx := context.Background()
	bdb, _ := newRawBolt(t)

	//lint:ignore SA1012 nil ctx must be rejected with ErrNilContext
	if _, err := Open(nil, bdb, Config{}); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Open(nil ctx) err = %v, want ErrNilContext", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := Open(cancelled, bdb, Config{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open(cancelled) err = %v, want context.Canceled", err)
	}

	if _, err := Open(ctx, nil, Config{}); !errors.Is(err, ErrNilParam) {
		t.Fatalf("Open(nil db) err = %v, want ErrNilParam", err)
	}

	if _, err := Open(ctx, bdb, Config{Root: [][]byte{[]byte("bad/name")}}); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("Open(bad root) err = %v, want ErrInvalidName", err)
	}

	// A name containing a NUL byte is rejected.
	if _, err := Open(ctx, bdb, Config{Root: [][]byte{[]byte("a\x00b")}}); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("Open(NUL root) err = %v, want ErrInvalidName", err)
	}
	// Dot and dot-dot root segments are rejected.
	for _, name := range []string{".", ".."} {
		if _, err := Open(ctx, bdb, Config{Root: [][]byte{[]byte(name)}}); !errors.Is(err, ErrInvalidName) {
			t.Fatalf("Open(root %q) err = %v, want ErrInvalidName", name, err)
		}
	}

	// A short secret makes key resolution fail.
	if _, err := Open(ctx, bdb, Config{EncryptValues: true, Secret: []byte("short")}); err == nil {
		t.Fatal("Open(short secret) err = nil, want key error")
	}
}

func TestOpenReadOnlyDoesNotMutate(t *testing.T) {
	ctx := context.Background()
	root := [][]byte{[]byte(bucket.SEARCH)}
	path := filepath.Join(t.TempDir(), "ro.db")

	// Create an index and a document with a writable handle.
	rw, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("bolt.Open rw: %v", err)
	}
	s, err := Open(ctx, rw, Config{Root: root})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	idx, err := s.Open("docs", Config{})
	if err != nil {
		t.Fatalf("Open index: %v", err)
	}
	if err := idx.Upsert(ctx, "a", "hello world"); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := rw.Close(); err != nil {
		t.Fatalf("close rw: %v", err)
	}

	// Reopen read-only and confirm reads work while writes are rejected.
	ro, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second, ReadOnly: true})
	if err != nil {
		t.Fatalf("bolt.Open ro: %v", err)
	}
	roStore, err := OpenReadOnly(ctx, ro, Config{Root: root})
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	roIdx, err := roStore.Open("docs", Config{})
	if err != nil {
		t.Fatalf("OpenReadOnly index: %v", err)
	}
	hits, err := roIdx.Search(ctx, "hello", 10)
	if err != nil {
		t.Fatalf("Search read-only: %v", err)
	}
	if len(hits) != 1 || hits[0].ID != "a" {
		t.Fatalf("Search read-only = %+v, want just a", hits)
	}

	// A missing index in read-only mode is not created.
	if _, err := roStore.Open("absent", Config{}); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("Open(missing) read-only err = %v, want ErrIndexNotFound", err)
	}

	// Writes must fail on the read-only handle.
	if err := roIdx.Upsert(ctx, "b", "second"); err == nil {
		t.Fatal("Upsert on read-only store succeeded, want error")
	}
	if err := roIdx.Delete(ctx, "a"); err == nil {
		t.Fatal("Delete on read-only store succeeded, want error")
	}
	if err := ro.Close(); err != nil {
		t.Fatalf("close ro: %v", err)
	}

	// Reopen read-write: the original document must survive unchanged.
	rw2, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("bolt.Open rw2: %v", err)
	}
	defer rw2.Close()
	s2, err := Open(ctx, rw2, Config{Root: root})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	idx2, err := s2.Open("docs", Config{})
	if err != nil {
		t.Fatalf("reopen index: %v", err)
	}
	count, err := idx2.Count(ctx)
	if err != nil || count != 1 {
		t.Fatalf("Count after read-only = %d err=%v, want 1", count, err)
	}
	if hits, _ := idx2.Search(ctx, "second", 10); len(hits) != 0 {
		t.Fatalf("read-only store leaked a write: %+v", hits)
	}
}

func TestWithTransactionScopedOperations(t *testing.T) {
	ctx := context.Background()
	s, bdb := testStore(t, Config{})
	idx := openIndex(t, s)
	if err := idx.Upsert(ctx, "a", "hello world"); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// A nil transaction yields no bound store.
	if s.WithTransaction(nil) != nil {
		t.Fatal("WithTransaction(nil) != nil")
	}

	err := bdb.Update(func(tx *bolt.Tx) error {
		txStore := s.WithTransaction(tx)
		if txStore == nil {
			t.Fatal("WithTransaction(tx) = nil")
		}
		txIdx, err := txStore.Open("docs", Config{})
		if err != nil {
			return err
		}
		if txIdx.Name() != "docs" {
			t.Errorf("tx index name = %q, want docs", txIdx.Name())
		}
		if err := txIdx.Upsert(ctx, "b", "another doc"); err != nil {
			return err
		}
		count, err := txIdx.Count(ctx)
		if err != nil {
			return err
		}
		if count != 2 {
			t.Errorf("tx Count = %d, want 2", count)
		}
		hits, err := txIdx.Search(ctx, "another", 10)
		if err != nil {
			return err
		}
		if len(hits) != 1 || hits[0].ID != "b" {
			t.Errorf("tx Search = %+v, want b", hits)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("transaction: %v", err)
	}

	// After commit the base index sees both documents.
	if count, _ := idx.Count(ctx); count != 2 {
		t.Fatalf("Count after commit = %d, want 2", count)
	}
}

func TestWithRootViews(t *testing.T) {
	ctx := context.Background()
	s, _ := testStore(t, Config{})

	// A nested root exercises multi-segment bucket creation and traversal.
	view := s.WithRoot("a", "b")
	if view == nil {
		t.Fatal("WithRoot returned nil")
	}
	idx, err := view.Open("idx", Config{})
	if err != nil {
		t.Fatalf("nested Open: %v", err)
	}
	if err := idx.Upsert(ctx, "k", "nested value"); err != nil {
		t.Fatalf("nested Upsert: %v", err)
	}
	if hits, _ := idx.Search(ctx, "nested", 10); len(hits) != 1 {
		t.Fatalf("nested Search = %+v", hits)
	}

	// Empty segments are ignored when building the root path.
	dbRoot := s.WithRoot("", "")
	if dbRoot == nil {
		t.Fatal("WithRoot(\"\",\"\") returned nil")
	}

	if (*Store)(nil).WithRoot("x") != nil {
		t.Fatal("nil store WithRoot != nil")
	}
}

// TestRootWithEmptySegment covers the empty-segment branches in rootBucket and
// createRootBucket.
func TestRootWithEmptySegment(t *testing.T) {
	ctx := context.Background()
	bdb, _ := newRawBolt(t)
	root := [][]byte{[]byte(bucket.SEARCH), {}}
	s, err := Open(ctx, bdb, Config{Root: root})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	idx, err := s.Open("docs", Config{})
	if err != nil {
		t.Fatalf("Open index: %v", err)
	}
	if err := idx.Upsert(ctx, "a", "value here"); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	names, err := s.Indexes(ctx)
	if err != nil {
		t.Fatalf("Indexes: %v", err)
	}
	if len(names) != 1 || names[0] != "docs" {
		t.Fatalf("Indexes = %v, want [docs]", names)
	}
}

func TestNilStoreReceivers(t *testing.T) {
	ctx := context.Background()
	var nilStore *Store

	if err := nilStore.view(func(*bolt.Tx) error { return nil }); !errors.Is(err, ErrNilParam) {
		t.Fatalf("nil view err = %v, want ErrNilParam", err)
	}
	if err := nilStore.update(func(*bolt.Tx) error { return nil }); !errors.Is(err, ErrNilParam) {
		t.Fatalf("nil update err = %v, want ErrNilParam", err)
	}
	if err := nilStore.Close(); err != nil {
		t.Fatalf("nil Close err = %v, want nil", err)
	}
	if nilStore.WithTransaction(nil) != nil {
		t.Fatal("nil WithTransaction != nil")
	}
	if _, err := nilStore.Open("x", Config{}); !errors.Is(err, ErrNilParam) {
		t.Fatalf("nil Open err = %v, want ErrNilParam", err)
	}
	if got := nilStore.encState(); got != nil {
		t.Fatalf("nil encState = %v, want nil", got)
	}

	// A non-nil store closes cleanly.
	s, _ := testStore(t, Config{})
	if err := s.Close(); err != nil {
		t.Fatalf("Close err = %v", err)
	}
	_ = ctx
}

func TestIndexNameAndClosed(t *testing.T) {
	ctx := context.Background()
	s, _ := testStore(t, Config{})
	idx := openIndex(t, s)
	if idx.Name() != "docs" {
		t.Fatalf("Name = %q, want docs", idx.Name())
	}
	var nilIdx *Index
	if nilIdx.Name() != "" {
		t.Fatalf("nil Name = %q, want empty", nilIdx.Name())
	}

	// Count on a missing index reports ErrIndexNotFound once its root is gone.
	if err := s.update(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(bucket.SEARCH))
		if root == nil {
			return errors.New("missing root")
		}
		return root.DeleteBucket([]byte("docs"))
	}); err != nil {
		t.Fatalf("delete index bucket: %v", err)
	}
	if _, err := idx.Count(ctx); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("Count(missing index) err = %v, want ErrIndexNotFound", err)
	}

	// And once the whole root is gone the bucket traversal returns not-found.
	if err := s.update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(bucket.SEARCH))
	}); err != nil {
		t.Fatalf("delete root bucket: %v", err)
	}
	if _, err := idx.Count(ctx); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("Count(missing root) err = %v, want ErrIndexNotFound", err)
	}
	// Searching a missing index surfaces the bucket error.
	if _, err := idx.Search(ctx, "hello", 10); err == nil {
		t.Fatal("Search(missing index) err = nil, want error")
	}
}

func TestCountEdgeCases(t *testing.T) {
	ctx := context.Background()
	s, _ := testStore(t, Config{})
	idx := openIndex(t, s)

	//lint:ignore SA1012 nil ctx must be rejected with ErrNilContext
	if _, err := idx.Count(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Count(nil) err = %v, want ErrNilContext", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := idx.Count(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Count(cancelled) err = %v, want context.Canceled", err)
	}

	if err := idx.Upsert(ctx, "a", "hello"); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	// Remove only the metadata so loadMeta reports a missing record.
	if err := s.update(func(tx *bolt.Tx) error {
		b, err := s.indexBucket(tx, "docs")
		if err != nil {
			return err
		}
		return b.Delete([]byte(metaKey))
	}); err != nil {
		t.Fatalf("delete meta: %v", err)
	}
	if count, err := idx.Count(ctx); err != nil || count != 0 {
		t.Fatalf("Count(no meta) = %d err=%v, want 0", count, err)
	}
}

func TestIndexesVariants(t *testing.T) {
	ctx := context.Background()

	// Flat store: indexes live at the database root.
	bdb, _ := newRawBolt(t)
	flat, err := Open(ctx, bdb, Config{})
	if err != nil {
		t.Fatalf("Open flat: %v", err)
	}
	if _, err := flat.Open("one", Config{}); err != nil {
		t.Fatalf("Open one: %v", err)
	}
	if _, err := flat.Open("two", Config{}); err != nil {
		t.Fatalf("Open two: %v", err)
	}
	names, err := flat.Indexes(ctx)
	if err != nil {
		t.Fatalf("Indexes flat: %v", err)
	}
	if len(names) != 2 || names[0] != "one" || names[1] != "two" {
		t.Fatalf("Indexes flat = %v, want [one two]", names)
	}
	// Exercise the flat indexBucket read path via Count/Search.
	one, err := flat.Open("one", Config{})
	if err != nil {
		t.Fatalf("Open one: %v", err)
	}
	if _, err := one.Count(ctx); err != nil {
		t.Fatalf("flat Count: %v", err)
	}
	// Underscore-prefixed top-level buckets are skipped by Indexes.
	if err := bdb.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("_internal"))
		return err
	}); err != nil {
		t.Fatalf("create underscore bucket: %v", err)
	}
	if names, err = flat.Indexes(ctx); err != nil || len(names) != 2 {
		t.Fatalf("Indexes flat after underscore = %v err=%v", names, err)
	}
	//lint:ignore SA1012 nil ctx must be rejected with ErrNilContext
	if _, err := flat.Indexes(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Indexes(nil) err = %v, want ErrNilContext", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := flat.Indexes(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Indexes(cancelled) err = %v, want context.Canceled", err)
	}

	// Rooted store whose root bucket was never created lists nothing.
	bdb2, _ := newRawBolt(t)
	rooted, err := Open(ctx, bdb2, Config{Root: [][]byte{[]byte(bucket.SEARCH)}})
	if err != nil {
		t.Fatalf("Open rooted: %v", err)
	}
	empty, err := rooted.Indexes(ctx)
	if err != nil {
		t.Fatalf("Indexes empty root: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("Indexes(empty root) = %v, want none", empty)
	}
}

func TestValidateBM25Ranges(t *testing.T) {
	s, _ := testStore(t, Config{})
	cases := []struct {
		name  string
		idx   string
		k1, b float64
	}{
		{"negative k1", "n1", -1, 0.75},
		{"negative b", "n2", 1.2, -0.1},
		{"b above one", "n3", 1.2, 1.5},
	}
	for _, tc := range cases {
		if _, err := s.Open(tc.idx, Config{K1: tc.k1, B: tc.b}); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%s: err = %v, want ErrInvalidConfig", tc.name, err)
		}
	}
}

func TestVersionAndModeErrors(t *testing.T) {
	ctx := context.Background()

	// A malformed (wrong length) version record fails closed.
	bdb, _ := newRawBolt(t)
	s, err := Open(ctx, bdb, Config{Root: [][]byte{[]byte(bucket.SEARCH)}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := s.Open("docs", Config{}); err != nil {
		t.Fatalf("Open index: %v", err)
	}
	if err := bdb.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucket.SEARCH)).Bucket([]byte("docs")).Put([]byte(versionKey), []byte{1, 2, 3})
	}); err != nil {
		t.Fatalf("corrupt version: %v", err)
	}
	if _, err := s.Open("docs", Config{}); !errors.Is(err, ErrIncompatibleVersion) {
		t.Fatalf("Open(malformed version) err = %v, want ErrIncompatibleVersion", err)
	}

	// Missing version under a read-only store is an incompatible version.
	bdb2, _ := newRawBolt(t)
	s2, err := Open(ctx, bdb2, Config{Root: [][]byte{[]byte(bucket.SEARCH)}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := s2.Open("docs", Config{}); err != nil {
		t.Fatalf("Open index: %v", err)
	}
	if err := bdb2.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucket.SEARCH)).Bucket([]byte("docs")).Delete([]byte(versionKey))
	}); err != nil {
		t.Fatalf("delete version: %v", err)
	}
	ro, err := OpenReadOnly(ctx, bdb2, Config{Root: [][]byte{[]byte(bucket.SEARCH)}})
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	if _, err := ro.Open("docs", Config{}); !errors.Is(err, ErrIncompatibleVersion) {
		t.Fatalf("read-only Open(missing version) err = %v, want ErrIncompatibleVersion", err)
	}

	// Missing mode record under a read-only store is the wrong mode.
	bdb3, _ := newRawBolt(t)
	s3, err := Open(ctx, bdb3, Config{Root: [][]byte{[]byte(bucket.SEARCH)}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := s3.Open("docs", Config{}); err != nil {
		t.Fatalf("Open index: %v", err)
	}
	if err := bdb3.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucket.SEARCH)).Bucket([]byte("docs")).Delete([]byte(modeKey))
	}); err != nil {
		t.Fatalf("delete mode: %v", err)
	}
	ro2, err := OpenReadOnly(ctx, bdb3, Config{Root: [][]byte{[]byte(bucket.SEARCH)}})
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	if _, err := ro2.Open("docs", Config{}); !errors.Is(err, ErrWrongMode) {
		t.Fatalf("read-only Open(missing mode) err = %v, want ErrWrongMode", err)
	}
}

func TestTermEncryptionRoundTrips(t *testing.T) {
	ctx := context.Background()
	secret := []byte("0123456789abcdef0123456789abcdef")

	// Token encryption without value encryption: term keys are encrypted but
	// values stay readable.
	bdb, _ := newRawBolt(t)
	s, err := Open(ctx, bdb, Config{Root: [][]byte{[]byte(bucket.SEARCH)}, Secret: secret, EncryptTokens: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	idx, err := s.Open("docs", Config{})
	if err != nil {
		t.Fatalf("Open index: %v", err)
	}
	if err := idx.Upsert(ctx, "a", "encrypted terms here"); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	again, err := Open(ctx, bdb, Config{Root: [][]byte{[]byte(bucket.SEARCH)}, Secret: secret, EncryptTokens: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	idx2, err := again.Open("docs", Config{})
	if err != nil {
		t.Fatalf("reopen index: %v", err)
	}
	if hits, _ := idx2.Search(ctx, "terms", 10); len(hits) != 1 || hits[0].ID != "a" {
		t.Fatalf("encrypted-token search = %+v, want a", hits)
	}
	// Plaintext open of an encrypted-token index is the wrong mode.
	plain, err := Open(ctx, bdb, Config{Root: [][]byte{[]byte(bucket.SEARCH)}})
	if err != nil {
		t.Fatalf("plain open: %v", err)
	}
	if _, err := plain.Open("docs", Config{}); !errors.Is(err, ErrWrongMode) {
		t.Fatalf("plain Open(encrypted-token index) err = %v, want ErrWrongMode", err)
	}
}

func TestDeleteUpsertEdges(t *testing.T) {
	ctx := context.Background()
	s, _ := testStore(t, Config{})
	idx := openIndex(t, s)

	//lint:ignore SA1012 nil ctx must be rejected with ErrNilContext
	if err := idx.Upsert(nil, "a", "x"); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Upsert(nil) err = %v, want ErrNilContext", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := idx.Upsert(cancelled, "a", "x"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Upsert(cancelled) err = %v, want context.Canceled", err)
	}
	if err := idx.Upsert(ctx, "", "x"); !errors.Is(err, ErrNilParam) {
		t.Fatalf("Upsert(empty id) err = %v, want ErrNilParam", err)
	}

	//lint:ignore SA1012 nil ctx must be rejected with ErrNilContext
	if err := idx.Delete(nil, "a"); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Delete(nil) err = %v, want ErrNilContext", err)
	}
	if err := idx.Delete(cancelled, "a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete(cancelled) err = %v, want context.Canceled", err)
	}
	if err := idx.Delete(ctx); err != nil {
		t.Fatalf("Delete() err = %v, want nil", err)
	}

	// Removing the index bucket makes later writes report not-found.
	if err := s.update(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(bucket.SEARCH))
		return root.DeleteBucket([]byte("docs"))
	}); err != nil {
		t.Fatalf("delete index bucket: %v", err)
	}
	if err := idx.Upsert(ctx, "a", "x"); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("Upsert(missing index) err = %v, want ErrIndexNotFound", err)
	}
	if err := idx.Delete(ctx, "a"); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("Delete(missing index) err = %v, want ErrIndexNotFound", err)
	}
}

func TestSearchAvgdlEdge(t *testing.T) {
	ctx := context.Background()
	s, _ := testStore(t, Config{})
	idx := openIndex(t, s)

	//lint:ignore SA1012 nil ctx must be rejected with ErrNilContext
	if _, err := idx.Search(nil, "x", 10); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Search(nil) err = %v, want ErrNilContext", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := idx.Search(cancelled, "x", 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("Search(cancelled) err = %v, want context.Canceled", err)
	}

	// A single empty document gives TotalLen 0, so avgdl is clamped to 1.
	if err := idx.Upsert(ctx, "empty", ""); err != nil {
		t.Fatalf("Upsert empty: %v", err)
	}
	if hits, err := idx.Search(ctx, "anything", 10); err != nil || len(hits) != 0 {
		t.Fatalf("Search(avgdl) = %+v err=%v, want none", hits, err)
	}
}

// bareTokenizer implements Tokenizer without a Name method.
type bareTokenizer struct{}

func (bareTokenizer) Tokenize(text string) []string { return Words().Tokenize(text) }

func TestTokenizerEdges(t *testing.T) {
	// isCJK covers every supported script plus a non-CJK rune.
	cjk := []rune{'中', '\u3400', '\uF900', 'あ', 'ア', '가', '\u1100'}
	for _, r := range cjk {
		if !isCJK(r) {
			t.Errorf("isCJK(%U) = false, want true", r)
		}
	}
	for _, r := range []rune{'a', '1', ' ', 'é'} {
		if isCJK(r) {
			t.Errorf("isCJK(%U) = true, want false", r)
		}
	}

	// The CJK tokenizer flushes across non-CJK text and short runs.
	terms := WordsWithCJK().Tokenize("abc 日本 def")
	if !containsTerm(terms, "日本") {
		t.Fatalf("short CJK run missing from %v", terms)
	}
	if !containsTerm(terms, "abc") || !containsTerm(terms, "def") {
		t.Fatalf("word terms missing from %v", terms)
	}

	// A tokenizer without a Name method is persisted as "custom".
	if got := tokenizerName(bareTokenizer{}); got != "custom" {
		t.Fatalf("tokenizerName(bare) = %q, want custom", got)
	}
}

func TestOpenReadOnlyErrors(t *testing.T) {
	ctx := context.Background()
	if _, err := OpenReadOnly(ctx, nil, Config{}); !errors.Is(err, ErrNilParam) {
		t.Fatalf("OpenReadOnly(nil db) err = %v, want ErrNilParam", err)
	}

	// A read-only open of an existing index with its metadata removed is not
	// found (the create path is unavailable).
	bdb, _ := newRawBolt(t)
	s, err := Open(ctx, bdb, Config{Root: [][]byte{[]byte(bucket.SEARCH)}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := s.Open("docs", Config{}); err != nil {
		t.Fatalf("Open index: %v", err)
	}
	if err := bdb.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucket.SEARCH)).Bucket([]byte("docs")).Delete([]byte(metaKey))
	}); err != nil {
		t.Fatalf("delete meta: %v", err)
	}
	ro, err := OpenReadOnly(ctx, bdb, Config{Root: [][]byte{[]byte(bucket.SEARCH)}})
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	if _, err := ro.Open("docs", Config{}); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("read-only Open(no meta) err = %v, want ErrIndexNotFound", err)
	}

	// A read-only open of an index missing its docs sub-bucket is not found.
	bdb2, _ := newRawBolt(t)
	s2, err := Open(ctx, bdb2, Config{Root: [][]byte{[]byte(bucket.SEARCH)}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := s2.Open("docs", Config{}); err != nil {
		t.Fatalf("Open index: %v", err)
	}
	if err := bdb2.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucket.SEARCH)).Bucket([]byte("docs")).DeleteBucket([]byte(docsBucket))
	}); err != nil {
		t.Fatalf("delete docs bucket: %v", err)
	}
	ro2, err := OpenReadOnly(ctx, bdb2, Config{Root: [][]byte{[]byte(bucket.SEARCH)}})
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	if _, err := ro2.Open("docs", Config{}); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("read-only Open(no docs) err = %v, want ErrIndexNotFound", err)
	}
}

func TestCorruptRecords(t *testing.T) {
	ctx := context.Background()

	// Corrupt metadata: reopening the index fails while decoding it.
	t.Run("meta", func(t *testing.T) {
		s, _ := testStore(t, Config{})
		if _, err := s.Open("docs", Config{}); err != nil {
			t.Fatalf("Open index: %v", err)
		}
		if err := s.update(func(tx *bolt.Tx) error {
			ib := tx.Bucket([]byte(bucket.SEARCH)).Bucket([]byte("docs"))
			return ib.Put([]byte(metaKey), []byte("{bad"))
		}); err != nil {
			t.Fatalf("corrupt meta: %v", err)
		}
		if _, err := s.Open("docs", Config{}); err == nil {
			t.Fatal("Open(corrupt meta) err = nil, want decode error")
		}
	})

	// Corrupt a document record: reading it back fails.
	t.Run("doc", func(t *testing.T) {
		s, _ := testStore(t, Config{})
		idx := openIndex(t, s)
		if err := idx.Upsert(ctx, "a", "hello"); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		if err := s.update(func(tx *bolt.Tx) error {
			db := tx.Bucket([]byte(bucket.SEARCH)).Bucket([]byte("docs")).Bucket([]byte(docsBucket))
			return db.Put([]byte("a"), []byte("{bad"))
		}); err != nil {
			t.Fatalf("corrupt doc: %v", err)
		}
		if _, err := idx.Search(ctx, "hello", 10); err == nil {
			t.Fatal("Search(corrupt doc) err = nil, want decode error")
		}
	})

	// Corrupt a posting list: search fails while decoding it.
	t.Run("term", func(t *testing.T) {
		s, _ := testStore(t, Config{})
		idx := openIndex(t, s)
		if err := idx.Upsert(ctx, "a", "hello"); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		if err := s.update(func(tx *bolt.Tx) error {
			tb := tx.Bucket([]byte(bucket.SEARCH)).Bucket([]byte("docs")).Bucket([]byte(termsBucket))
			return tb.Put([]byte("hello"), []byte("{bad"))
		}); err != nil {
			t.Fatalf("corrupt term: %v", err)
		}
		if _, err := idx.Search(ctx, "hello", 10); err == nil {
			t.Fatal("Search(corrupt term) err = nil, want decode error")
		}
	})
}
